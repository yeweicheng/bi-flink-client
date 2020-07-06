/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yewc.flink.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.*;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.*;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.yewc.flink.yarn.CliFrontendParser.HELP_OPTION;

/**
 * Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {

    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

    // actions
    private static final String ACTION_RUN = "run";
    private static final String ACTION_INFO = "info";
    private static final String ACTION_LIST = "list";
    private static final String ACTION_CANCEL = "cancel";
    private static final String ACTION_STOP = "stop";
    private static final String ACTION_SAVEPOINT = "savepoint";

    // configuration dir parameters
    private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
    private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

    // --------------------------------------------------------------------------------------------

    private final Configuration configuration;

    private final List<CustomCommandLine> customCommandLines;

    private final Options customCommandLineOptions;

    private final Duration clientTimeout;

    private final int defaultParallelism;

    private final ClusterClientServiceLoader clusterClientServiceLoader;

    private final ClusterClientFactory<ApplicationId> clusterClientFactory;

    private final Map<String, ClusterClient<ApplicationId>> jmClientMap = new ConcurrentHashMap<>();

    public CliFrontend(
            Configuration configuration,
            List<CustomCommandLine> customCommandLines) {
        this(configuration, new DefaultClusterClientServiceLoader(), customCommandLines);
    }

    public CliFrontend(
            Configuration configuration,
            ClusterClientServiceLoader clusterClientServiceLoader,
            List<CustomCommandLine> customCommandLines) {
        this.configuration = checkNotNull(configuration);
        this.customCommandLines = checkNotNull(customCommandLines);
        this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);

        FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        this.customCommandLineOptions = new Options();

        for (CustomCommandLine customCommandLine : customCommandLines) {
            customCommandLine.addGeneralOptions(customCommandLineOptions);
            customCommandLine.addRunOptions(customCommandLineOptions);
        }

        this.configuration.setString(DeploymentOptions.TARGET, "yarn-per-job");

        this.clientTimeout = AkkaUtils.getClientTimeout(this.configuration);
        this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
        this.clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter & Setter
    // --------------------------------------------------------------------------------------------

    /**
     * Getter which returns a copy of the associated configuration.
     *
     * @return Copy of the associated configuration
     */
    public Configuration getConfiguration() {
        Configuration copiedConfiguration = new Configuration();

        copiedConfiguration.addAll(configuration);

        return copiedConfiguration;
    }

    public Options getCustomCommandLineOptions() {
        return customCommandLineOptions;
    }

    // --------------------------------------------------------------------------------------------
    //  Execute Actions
    // --------------------------------------------------------------------------------------------

    /**
     * Executions the run action.
     *
     * @param args Command line arguments for the run action.
     */
    protected JSONObject run(String[] args) throws Exception {
        LOG.info("Running 'run' command.");

        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, args, true);

        final ProgramOptions programOptions = new ProgramOptions(commandLine);

        if (!programOptions.isPython()) {
            // Java program should be specified a JAR file
            if (programOptions.getJarFilePath() == null) {
                throw new CliArgsException("Java program should be specified a JAR file.");
            }
        }

        final PackagedProgram program;
        try {
            LOG.info("Building program from JAR file");
            program = buildProgram(programOptions);
        }
        catch (FileNotFoundException e) {
            throw new CliArgsException("Could not build the program from JAR file.", e);
        }

        List<URL> jobJars = program.getJobJarAndDependencies();
        jobJars.addAll(program.getClasspaths());
        final Configuration effectiveConfiguration =
                getEffectiveConfiguration(commandLine, programOptions, jobJars);

        LOG.debug("Effective executor configuration: {}", effectiveConfiguration);

        try {
            executeProgram(effectiveConfiguration, program);
        } finally {
            program.deleteExtractedLibraries();
        }

        return new JSONObject(effectiveConfiguration.toMap());
    }

    private Configuration getEffectiveConfiguration(
            final CommandLine commandLine,
            final ProgramOptions programOptions,
            final List<URL> jobJars) throws FlinkException {

        final CustomCommandLine customCommandLine = getActiveCustomCommandLine(checkNotNull(commandLine));
        final ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(
                checkNotNull(programOptions),
                checkNotNull(jobJars));

        final Configuration executorConfig = customCommandLine.applyCommandLineOptionsToConfiguration(commandLine);
        final Configuration effectiveConfiguration = new Configuration(executorConfig);
        return executionParameters.applyToConfiguration(effectiveConfiguration);
    }

    /**
     * Executes the info action.
     *
     * @param args Command line arguments for the info action.
     */
    protected void info(String[] args) throws Exception {
        LOG.info("Running 'info' command.");

        final Options commandOptions = CliFrontendParser.getInfoCommandOptions();

        final CommandLine commandLine = CliFrontendParser.parse(commandOptions, args, true);

        final ProgramOptions programOptions = new ProgramOptions(commandLine);

        // evaluate help flag
        if (commandLine.hasOption(HELP_OPTION.getOpt())) {
            CliFrontendParser.printHelpForInfo();
            return;
        }

        if (programOptions.getJarFilePath() == null) {
            throw new CliArgsException("The program JAR file was not specified.");
        }

        // -------- build the packaged program -------------

        LOG.info("Building program from JAR file");
        final PackagedProgram program = buildProgram(programOptions);

        try {
            int parallelism = programOptions.getParallelism();
            if (ExecutionConfig.PARALLELISM_DEFAULT == parallelism) {
                parallelism = defaultParallelism;
            }

            LOG.info("Creating program plan dump");

            final Configuration effectiveConfiguration =
                    getEffectiveConfiguration(commandLine, programOptions, program.getJobJarAndDependencies());

            Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(program, parallelism, true);
            String jsonPlan = FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(pipeline);

            if (jsonPlan != null) {
                System.out.println("----------------------- Execution Plan -----------------------");
                System.out.println(jsonPlan);
                System.out.println("--------------------------------------------------------------");
            }
            else {
                System.out.println("JSON plan could not be generated.");
            }

            String description = program.getDescription();
            if (description != null) {
                System.out.println();
                System.out.println(description);
            }
            else {
                System.out.println();
                System.out.println("No description provided.");
            }
        }
        finally {
            program.deleteExtractedLibraries();
        }
    }

    /**
     * Executes the list action.
     *
     * @param args Command line arguments for the list action.
     */
    protected JSONObject list(String[] args) throws Exception {
        LOG.info("Running 'list' command.");

        final Options commandOptions = CliFrontendParser.getListCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, args, false);

        JSONObject callback = new JSONObject();
        final ClusterDescriptor<ApplicationId> clusterDescriptor = clusterClientFactory.createClusterDescriptor(this.configuration);
        YarnClient yarnClient = ((YarnClusterDescriptor) clusterDescriptor).getYarnClient();

        String yid = commandLine.getOptionValue("yid");
        if (StringUtils.isBlank(yid)) {
            EnumSet<YarnApplicationState> stateSet = EnumSet.noneOf(YarnApplicationState.class);
            stateSet.add(YarnApplicationState.SUBMITTED);
            stateSet.add(YarnApplicationState.ACCEPTED);
            stateSet.add(YarnApplicationState.RUNNING);

            List<ApplicationReport> applicationReports = yarnClient.getApplications(stateSet);
            for (int i = 0; i < applicationReports.size(); i++) {
                ApplicationReport report = applicationReports.get(i);
                handleReport(callback, report);
            }
        } else {
            final ApplicationId aid = ConverterUtils.toApplicationId(yid);
            handleReport(callback, yarnClient.getApplicationReport(aid));

        }

        return callback;
    }

    private void handleReport(JSONObject callback, ApplicationReport report) {
        ApplicationId aid = report.getApplicationId();
        String aidStr = ConverterUtils.toString(aid);
        JSONObject tempJo = new JSONObject();
        tempJo.put("code", 0);
        tempJo.put("finalApplicationStatus", report.getFinalApplicationStatus().name());
        tempJo.put("yarnApplicationStatus", report.getYarnApplicationState().name());
        if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
            tempJo.put("finishTime", report.getFinishTime());
            callback.put(aidStr, tempJo);
            return;
        }
        String host = report.getHost();
        if ("N/A".equals(host)) {
            callback.put(aidStr, tempJo);
            return;
        }

        try {
            int port = report.getRpcPort();
            Configuration flinkConfiguration = new Configuration();
            flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
            flinkConfiguration.setInteger(JobManagerOptions.PORT, port);
            flinkConfiguration.setString(RestOptions.ADDRESS, host);
            flinkConfiguration.setInteger(RestOptions.PORT, port);
            flinkConfiguration.set(YarnConfigOptions.APPLICATION_ID, aidStr);
            flinkConfiguration.set(RestOptions.RETRY_MAX_ATTEMPTS, 0);
            flinkConfiguration.set(RestOptions.AWAIT_LEADER_TIMEOUT, 3000L);

            String jmKey = host + ":" + port;
            ClusterClient<ApplicationId> clusterClient = null;
            if (jmClientMap.containsKey(jmKey)) {
                clusterClient = jmClientMap.get(jmKey);
            } else {
                clusterClient = new RestClusterClient(flinkConfiguration, aid);
                jmClientMap.put(jmKey, clusterClient);
            }

            Collection<JobStatusMessage> jobList = clusterClient.listJobs().get();
            if (jobList.size() != 1) {
                LOG.warn("yarn " + aidStr + " has " + jobList.size()
                        + " jobs not equals one, it should not be happen.");
                throw new RuntimeException("wrong jobs size: " + jobList.size());
            }

            for (JobStatusMessage jsm : jobList) {
                tempJo.put("jobName", jsm.getJobName());
                tempJo.put("jobId", jsm.getJobId().toHexString());
                tempJo.put("state", jsm.getJobState().name());
                break;
            }
            tempJo.put("code", 0);

        } catch (Exception e) {
            tempJo.put("code", 1);
            tempJo.put("msg", e.getMessage());
            LOG.error(aidStr, e);
        }

        callback.put(aidStr, tempJo);
    }

    /**
     * Executes the STOP action.
     *
     * @param args Command line arguments for the stop action.
     */
    protected JSONObject stop(String[] args) throws Exception {
        LOG.info("Running 'stop-with-savepoint' command.");

        final Options commandOptions = CliFrontendParser.getStopCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, args, false);

        final StopOptions stopOptions = new StopOptions(commandLine);

        final String[] cleanedArgs = stopOptions.getArgs();

        final String targetDirectory = stopOptions.hasSavepointFlag() && cleanedArgs.length > 0
                ? stopOptions.getTargetDirectory()
                : null; // the default savepoint location is going to be used in this case.

        final JobID jobId = cleanedArgs.length != 0
                ? parseJobId(cleanedArgs[0])
                : parseJobId(stopOptions.getTargetDirectory());

        final boolean advanceToEndOfEventTime = stopOptions.shouldAdvanceToEndOfEventTime();

        logAndSysout((advanceToEndOfEventTime ? "Draining job " : "Suspending job ") + "\"" + jobId + "\" with a savepoint.");

        final CustomCommandLine activeCommandLine = getActiveCustomCommandLine(commandLine);
        runClusterAction(
                activeCommandLine,
                commandLine,
                clusterClient -> {
                    final String savepointPath;
                    try {
                        savepointPath = clusterClient.stopWithSavepoint(jobId, advanceToEndOfEventTime, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        throw new FlinkException("Could not stop with a savepoint job \"" + jobId + "\".", e);
                    }
                    logAndSysout("Savepoint completed. Path: " + savepointPath);
                });

        return null;
    }

    /**
     * Executes the CANCEL action.
     *
     * @param args Command line arguments for the cancel action.
     */
    protected JSONObject cancel(String[] args) throws Exception {
        LOG.info("Running 'cancel' command.");

        final Options commandOptions = CliFrontendParser.getCancelCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, args, false);

        CancelOptions cancelOptions = new CancelOptions(commandLine);

        final CustomCommandLine activeCommandLine = getActiveCustomCommandLine(commandLine);

        final String[] cleanedArgs = cancelOptions.getArgs();

        if (cancelOptions.isWithSavepoint()) {

            logAndSysout("DEPRECATION WARNING: Cancelling a job with savepoint is deprecated. Use \"stop\" instead.");

            final JobID jobId;
            final String targetDirectory;

            if (cleanedArgs.length > 0) {
                jobId = parseJobId(cleanedArgs[0]);
                targetDirectory = cancelOptions.getSavepointTargetDirectory();
            } else {
                jobId = parseJobId(cancelOptions.getSavepointTargetDirectory());
                targetDirectory = null;
            }

            if (targetDirectory == null) {
                logAndSysout("Cancelling job " + jobId + " with savepoint to default savepoint directory.");
            } else {
                logAndSysout("Cancelling job " + jobId + " with savepoint to " + targetDirectory + '.');
            }

            runClusterAction(
                    activeCommandLine,
                    commandLine,
                    clusterClient -> {
                        final String savepointPath;
                        try {
                            savepointPath = clusterClient.cancelWithSavepoint(jobId, targetDirectory).get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            throw new FlinkException("Could not cancel job " + jobId + '.', e);
                        }
                        logAndSysout("Cancelled job " + jobId + ". Savepoint stored in " + savepointPath + '.');
                    });
        } else {
            final JobID jobId;

            if (cleanedArgs.length > 0) {
                jobId = parseJobId(cleanedArgs[0]);
            } else {
                throw new CliArgsException("Missing JobID. Specify a JobID to cancel a job.");
            }

            logAndSysout("Cancelling job " + jobId + '.');

            runClusterAction(
                    activeCommandLine,
                    commandLine,
                    clusterClient -> {
                        try {
                            clusterClient.cancel(jobId).get(3, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            if (!(e instanceof TimeoutException)) {
                                throw new FlinkException("Could not cancel job " + jobId + '.', e);
                            }

                            try {
                                String yid = commandLine.getOptionValue("yid");
                                String[] listArgs = {"list", "-yid", yid};
                                JSONObject listCallBack = list(listArgs);
                                String finalApplicationStatus = listCallBack.getJSONObject(yid).getString("finalApplicationStatus");
                                if (!"KILLED".equals(finalApplicationStatus) && !"SUCCEEDED".equals(finalApplicationStatus)) {
                                    throw new FlinkException("Could not cancel job " + jobId + '.', e);
                                }
                            } catch (Exception e1) {
                                throw new FlinkException("Could not cancel job " + jobId + '.', e1);
                            }
                        }
                    });

            logAndSysout("Cancelled job " + jobId + '.');
        }
        return new JSONObject();
    }

    public CommandLine getCommandLine(final Options commandOptions, final String[] args, final boolean stopAtNonOptions) throws CliArgsException {
        final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
        return CliFrontendParser.parse(commandLineOptions, args, stopAtNonOptions);
    }

    /**
     * Executes the SAVEPOINT action.
     *
     * @param args Command line arguments for the savepoint action.
     */
    protected void savepoint(String[] args) throws Exception {
        LOG.info("Running 'savepoint' command.");

        final Options commandOptions = CliFrontendParser.getSavepointCommandOptions();

        final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

        final CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, false);

        final SavepointOptions savepointOptions = new SavepointOptions(commandLine);

        // evaluate help flag
        if (savepointOptions.isPrintHelp()) {
            CliFrontendParser.printHelpForSavepoint(customCommandLines);
            return;
        }

        final CustomCommandLine activeCommandLine = getActiveCustomCommandLine(commandLine);

        if (savepointOptions.isDispose()) {
            runClusterAction(
                    activeCommandLine,
                    commandLine,
                    clusterClient -> disposeSavepoint(clusterClient, savepointOptions.getSavepointPath()));
        } else {
            String[] cleanedArgs = savepointOptions.getArgs();

            final JobID jobId;

            if (cleanedArgs.length >= 1) {
                String jobIdString = cleanedArgs[0];

                jobId = parseJobId(jobIdString);
            } else {
                throw new CliArgsException("Missing JobID. " +
                        "Specify a Job ID to trigger a savepoint.");
            }

            final String savepointDirectory;
            if (cleanedArgs.length >= 2) {
                savepointDirectory = cleanedArgs[1];
            } else {
                savepointDirectory = null;
            }

            // Print superfluous arguments
            if (cleanedArgs.length >= 3) {
                logAndSysout("Provided more arguments than required. Ignoring not needed arguments.");
            }

            runClusterAction(
                    activeCommandLine,
                    commandLine,
                    clusterClient -> triggerSavepoint(clusterClient, jobId, savepointDirectory));
        }

    }

    /**
     * Sends a SavepointTriggerMessage to the job manager.
     */
    private void triggerSavepoint(ClusterClient<?> clusterClient, JobID jobId, String savepointDirectory) throws FlinkException {
        logAndSysout("Triggering savepoint for job " + jobId + '.');

        CompletableFuture<String> savepointPathFuture = clusterClient.triggerSavepoint(jobId, savepointDirectory);

        logAndSysout("Waiting for response...");

        try {
            final String savepointPath = savepointPathFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);

            logAndSysout("Savepoint completed. Path: " + savepointPath);
            logAndSysout("You can resume your program from this savepoint with the run command.");
        } catch (Exception e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            throw new FlinkException("Triggering a savepoint for the job " + jobId + " failed.", cause);
        }
    }

    /**
     * Sends a SavepointDisposalRequest to the job manager.
     */
    private void disposeSavepoint(ClusterClient<?> clusterClient, String savepointPath) throws FlinkException {
        checkNotNull(savepointPath, "Missing required argument: savepoint path. " +
                "Usage: bin/flink savepoint -d <savepoint-path>");

        logAndSysout("Disposing savepoint '" + savepointPath + "'.");

        final CompletableFuture<Acknowledge> disposeFuture = clusterClient.disposeSavepoint(savepointPath);

        logAndSysout("Waiting for response...");

        try {
            disposeFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new FlinkException("Disposing the savepoint '" + savepointPath + "' failed.", e);
        }

        logAndSysout("Savepoint '" + savepointPath + "' disposed.");
    }

    // --------------------------------------------------------------------------------------------
    //  Interaction with programs and JobManager
    // --------------------------------------------------------------------------------------------

    protected void executeProgram(final Configuration configuration, final PackagedProgram program) throws ProgramInvocationException {
        ClientUtils.executeProgram(DefaultExecutorServiceLoader.INSTANCE, configuration, program);
    }

    /**
     * Creates a Packaged program from the given command line options.
     *
     * @return A PackagedProgram (upon success)
     */
    PackagedProgram buildProgram(final ProgramOptions runOptions) throws FileNotFoundException, ProgramInvocationException {
        String[] programArgs = runOptions.getProgramArgs();
        String jarFilePath = runOptions.getJarFilePath();
        List<URL> classpaths = runOptions.getClasspaths();

        // Get assembler class
        String entryPointClass = runOptions.getEntryPointClassName();
        File jarFile = null;
        if (runOptions.isPython()) {
            // If the job is specified a jar file
            if (jarFilePath != null) {
                jarFile = getJarFile(jarFilePath);
            }

            // If the job is Python Shell job, the entry point class name is PythonGateWayServer.
            // Otherwise, the entry point class of python job is PythonDriver
            if (entryPointClass == null) {
                entryPointClass = "org.apache.flink.client.python.PythonDriver";
            }
        } else {
            if (jarFilePath == null) {
                throw new IllegalArgumentException("Java program should be specified a JAR file.");
            }
            jarFile = getJarFile(jarFilePath);
        }

        return PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setUserClassPaths(classpaths)
                .setEntryPointClassName(entryPointClass)
                .setConfiguration(configuration)
                .setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
                .setArguments(programArgs)
                .build();
    }

    /**
     * Gets the JAR file from the path.
     *
     * @param jarFilePath The path of JAR file
     * @return The JAR file
     * @throws FileNotFoundException The JAR file does not exist.
     */
    private File getJarFile(String jarFilePath) throws FileNotFoundException {
        File jarFile = new File(jarFilePath);
        // Check if JAR file exists
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
        }
        else if (!jarFile.isFile()) {
            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
        }
        return jarFile;
    }

    // --------------------------------------------------------------------------------------------
    //  Logging and Exception Handling
    // --------------------------------------------------------------------------------------------

    /**
     * Displays an exception message for incorrect command line arguments.
     *
     * @param e The exception to display.
     * @return The return code for the process.
     */
    private static String handleArgException(CliArgsException e) {
        LOG.error("Invalid command line arguments.", e);
        return e.getMessage();
    }

    /**
     * Displays an optional exception message for incorrect program parametrization.
     *
     * @param e The exception to display.
     * @return The return code for the process.
     */
    private static String handleParametrizationException(ProgramParametrizationException e) {
        LOG.error("Program has not been parametrized properly.", e);
        return e.getMessage();
    }

    /**
     * Displays a message for a program without a job to execute.
     *
     * @return The return code for the process.
     */
    private static String handleMissingJobException() {
        return "The program didn't contain a Flink job. " +
                "Perhaps you forgot to call execute() on the execution environment.";
    }

    /**
     * Displays an exception message.
     *
     * @param t The exception to display.
     * @return The return code for the process.
     */
    private static String handleError(Throwable t) {
        LOG.error("Error while running the command.", t);

        StringBuilder sb = new StringBuilder();
        if (t.getCause() instanceof InvalidProgramException) {
            sb.append(t.getCause().getMessage());
            StackTraceElement[] trace = t.getCause().getStackTrace();
            for (StackTraceElement ele: trace) {
                sb.append("\t" + ele);
                if (ele.getMethodName().equals("main")) {
                    break;
                }
            }
        } else {
            sb.append(t.getMessage());
        }
        return sb.toString();
    }

    private static void logAndSysout(String message) {
        LOG.info(message);
        System.out.println(message);
    }

    // --------------------------------------------------------------------------------------------
    //  Internal methods
    // --------------------------------------------------------------------------------------------

    private JobID parseJobId(String jobIdString) throws CliArgsException {
        if (jobIdString == null) {
            throw new CliArgsException("Missing JobId");
        }

        final JobID jobId;
        try {
            jobId = JobID.fromHexString(jobIdString);
        } catch (IllegalArgumentException e) {
            throw new CliArgsException(e.getMessage());
        }
        return jobId;
    }

    /**
     * Retrieves the {@link ClusterClient} from the given {@link CustomCommandLine} and runs the given
     * {@link ClusterAction} against it.
     *
     * @param activeCommandLine to create the {@link ClusterDescriptor} from
     * @param commandLine containing the parsed command line options
     * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
     * @param <ClusterID> type of the cluster id
     * @throws FlinkException if something goes wrong
     */
    private <ClusterID> void runClusterAction(CustomCommandLine activeCommandLine, CommandLine commandLine, ClusterAction<ClusterID> clusterAction) throws FlinkException {
        final Configuration executorConfig = activeCommandLine.applyCommandLineOptionsToConfiguration(commandLine);
        final ClusterClientFactory<ClusterID> clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(executorConfig);

        final ClusterID clusterId = clusterClientFactory.getClusterId(executorConfig);
        if (clusterId == null) {
            throw new FlinkException("No cluster id was specified. Please specify a cluster to which you would like to connect.");
        }

        try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(executorConfig)) {
            try (final ClusterClient<ClusterID> clusterClient = clusterDescriptor.retrieve(clusterId).getClusterClient()) {
                clusterAction.runAction(clusterClient);
            }
        }
    }

    /**
     * Internal interface to encapsulate cluster actions which are executed via
     * the {@link ClusterClient}.
     *
     * @param <ClusterID> type of the cluster id
     */
    @FunctionalInterface
    private interface ClusterAction<ClusterID> {

        /**
         * Run the cluster action with the given {@link ClusterClient}.
         *
         * @param clusterClient to run the cluster action against
         * @throws FlinkException if something goes wrong
         */
        void runAction(ClusterClient<ClusterID> clusterClient) throws FlinkException;
    }

    // --------------------------------------------------------------------------------------------
    //  Entry point for executable
    // --------------------------------------------------------------------------------------------

    /**
     * Parses the command line arguments and starts the requested action.
     *
     * @param args command line arguments of the client.
     * @return The return code of the program
     */
    public JSONObject parseParameters(String[] args) {
        JSONObject callback = new JSONObject();
        callback.put("code", 0);

        // check for action
        if (args.length < 1) {
            CliFrontendParser.printHelp(customCommandLines);
            callback.put("code", 1);
            callback.put("msg", "Please specify an action.");
            return callback;
        }

        // get action
        String action = args[0];

        // remove action from parameters
        final String[] params = Arrays.copyOfRange(args, 1, args.length);

        try {
            // do action
            switch (action) {
                case ACTION_RUN:
                    callback = run(params);
                    callback.put("code", 0);
                    break;
                case ACTION_LIST:
                    callback = list(params);
                    callback.put("code", 0);
                    break;
                case ACTION_INFO:
                    info(params);
                    break;
                case ACTION_CANCEL:
                    callback = cancel(params);
                    callback.put("code", 0);
                    break;
                case ACTION_STOP:
                    callback = stop(params);
                    callback.put("code", 0);
                    break;
                case ACTION_SAVEPOINT:
                    savepoint(params);
                    break;
                default:
                    callback.put("code", 1);
                    callback.put("msg", "Unknow action.");
                    break;
            }
        } catch (CliArgsException ce) {
            callback.put("code", 1);
            callback.put("msg", handleArgException(ce));
        } catch (ProgramParametrizationException ppe) {
            callback.put("code", 1);
            callback.put("msg", handleParametrizationException(ppe));
        } catch (ProgramMissingJobException pmje) {
            callback.put("code", 1);
            callback.put("msg", handleMissingJobException());
        } catch (Exception e) {
            callback.put("code", 1);
            callback.put("msg", handleError(e));
        }

        return callback;
    }

    /**
     * Submits the job based on the arguments.
     */
    public static JSONObject handle(final JSONObject params) {
        if (params.has("properties")) {
            JSONObject properties = params.getJSONObject("properties");
            for (String key : properties.keySet()) {
                System.setProperty(key, properties.getString(key));
            }
        }

        final String[] args = (String[]) params.get("args");

        // 1. find the configuration directory
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        // 3. load the custom command lines
        final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
                configuration,
                configurationDirectory);

        JSONObject callback = null;
        try {
            final CliFrontend cli = new CliFrontend(
                    configuration,
                    customCommandLines);

            SecurityUtils.install(new SecurityConfiguration(cli.configuration));
            callback = SecurityUtils.getInstalledContext()
                    .runSecured(() -> cli.parseParameters(args));
        }
        catch (Throwable t) {
            final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            LOG.error("Fatal error while running command line interface.", strippedThrowable);
            strippedThrowable.printStackTrace();
            callback.put("code", 1);
            callback.put("msg", strippedThrowable.getMessage());
        }

        return callback;
    }

    public static void main(String[] args) throws Exception {
        JSONObject params = new JSONObject();
        params.put("args", "cancel -yid application_1590640847799_0005".split(" "));
        System.out.println(CliFrontend.handle(params));
    }

    // --------------------------------------------------------------------------------------------
    //  Miscellaneous Utilities
    // --------------------------------------------------------------------------------------------

    public static String getConfigurationDirectoryFromEnv() {
        String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

        if (location != null) {
            if (new File(location).exists()) {
                return location;
            }
            else {
                throw new RuntimeException("The configuration directory '" + location + "', specified in the '" +
                        ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable, does not exist.");
            }
        }
        else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_1;
        }
        else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_2;
        }
        else {
            throw new RuntimeException("The configuration directory was not specified. " +
                    "Please specify the directory containing the configuration file through the '" +
                    ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable.");
        }
        return location;
    }

    /**
     * Writes the given job manager address to the associated configuration object.
     *
     * @param address Address to write to the configuration
     * @param config The configuration to write to
     */
    static void setJobManagerAddressInConfig(Configuration config, InetSocketAddress address) {
        config.setString(JobManagerOptions.ADDRESS, address.getHostString());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        config.setString(RestOptions.ADDRESS, address.getHostString());
        config.setInteger(RestOptions.PORT, address.getPort());
    }

    public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
        List<CustomCommandLine> customCommandLines = new ArrayList<>();
        customCommandLines.add(new ExecutorCLI(configuration));

        //	Command line interface of the YARN session, with a special initialization here
        //	to prefix all options with y/yarn.
        final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
        try {
            customCommandLines.add(
                    loadCustomCommandLine(flinkYarnSessionCLI,
                            configuration,
                            configurationDirectory,
                            "y",
                            "yarn"));
        } catch (NoClassDefFoundError | Exception e) {
            LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
        }

        //	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
        //	      active CustomCommandLine in order and DefaultCLI isActive always return true.
        customCommandLines.add(new DefaultCLI(configuration));

        return customCommandLines;
    }

    // --------------------------------------------------------------------------------------------
    //  Custom command-line
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the custom command-line for the arguments.
     * @param commandLine The input to the command-line.
     * @return custom command-line which is active (may only be one at a time)
     */
    public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
        LOG.debug("Custom commandlines: {}", customCommandLines);
        for (CustomCommandLine cli : customCommandLines) {
            LOG.debug("Checking custom commandline {}, isActive: {}", cli, cli.isActive(commandLine));
            if (cli.isActive(commandLine)) {
                return cli;
            }
        }
        throw new IllegalStateException("No command-line ran.");
    }

    /**
     * Loads a class from the classpath that implements the CustomCommandLine interface.
     * @param className The fully-qualified class name to load.
     * @param params The constructor parameters
     */
    private static CustomCommandLine loadCustomCommandLine(String className, Object... params) throws Exception {

        Class<? extends CustomCommandLine> customCliClass =
                Class.forName(className).asSubclass(CustomCommandLine.class);

        // construct class types from the parameters
        Class<?>[] types = new Class<?>[params.length];
        for (int i = 0; i < params.length; i++) {
            checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
            types[i] = params[i].getClass();
        }

        Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

        return constructor.newInstance(params);
    }

}
