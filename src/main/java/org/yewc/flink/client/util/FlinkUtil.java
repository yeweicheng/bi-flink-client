package org.yewc.flink.client.util;

import com.squareup.okhttp.RequestBody;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.flink.yarn.CliFrontend;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FlinkUtil {

    public static final Logger logger = LoggerFactory.getLogger(FlinkUtil.class);

    private static final String SUBMIT_JOB = "http://%s/jars/%s/run";

    private static final String JAR_LIST = "http://%s/jars";

    private static final String POINT_JOB = "http://%s/jobs/%s";

    private static final String SAVEPOINT_JOB = "http://%s/jobs/%s/savepoints";

    private static final String STATUS_JOB = "http://%s/jobs/overview";

    private static final String STATUS_CURRENT_JOB = "http://%s/jobs";

    private static final String STATUS_THE_JOB = "http://%s/jobs/%s/exceptions";

    public static final String FIELD_NAME_ENTRY_CLASS = "entryClass";
    public static final String FIELD_NAME_PROGRAM_ARGUMENTS = "programArgs";
    public static final String FIELD_NAME_PROGRAM_ARGUMENTS_LIST = "programArgsList";
    public static final String FIELD_NAME_PARALLELISM = "parallelism";
    public static final String FIELD_NAME_SAVEPOINTPATH = "savepointPath";
    public static final String FIELD_NAME_ALLOWNONRESTOREDSTATE = "allowNonRestoredState";

    private static final Map<String, ClusterClient> clientMap = new HashMap<>(2);

    public static String getJarId(String jobManager, String jarName) throws Exception {
        JSONArray files = getJarList(jobManager);

        String rightId = null;
        long rightUploadedTime = 0L;
        for (int i = 0; i < files.length(); i++) {
            JSONObject file = files.getJSONObject(i);
            if (file.getString("name").equals(jarName) && file.getLong("uploaded") > rightUploadedTime) {
                rightId = file.getString("id");
                rightUploadedTime = file.getLong("uploaded");
            }
        }

        return rightId;
    }

    public static JSONArray getJarList(String jobManager) throws Exception {
        String url = String.format(JAR_LIST, jobManager);
        JSONObject result = new JSONObject(HttpReqUtil.get(url, true));
        JSONArray files = result.getJSONArray("files");
        return files;
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }

    public static String submit(JSONObject requestParams, ExecuteType executeType) throws Exception {
        JSONObject clientParams = requestParams.getJSONObject("clientParams");
        JSONObject systemParams = requestParams.getJSONObject("systemParams");

        if (executeType.equals(ExecuteType.YARN)) {
//            final String[] argsx = {"run", "-m", "yarn-cluster", "-p", "2", "-yjm", "1024m", "-ytm", "1024m", "WordCount.jar"};
            String yarnRunCommand = systemParams.getString("yarnRunCommand");

            // 一般参数
            JSONObject yarnParamValues = systemParams.getJSONObject("yarnParamValues");
            for (String key : yarnParamValues.keySet()) {
                yarnRunCommand = yarnRunCommand.replaceAll("\\$" + key, yarnParamValues.getString(key));
            }

            //作业名
            yarnRunCommand = yarnRunCommand.replaceAll("\\$jobName", requestParams.getString("jobName"));

            // 并发
            Integer parallelism = clientParams.getInt(FlinkUtil.FIELD_NAME_PARALLELISM);
            yarnRunCommand = yarnRunCommand.replaceAll("\\$parallelism", parallelism.toString());

            String flinkSdkHdfsPath = systemParams.getString("flinkSdkHdfsPath");
            String flinkClasspathHdfsPath = systemParams.getString("flinkClasspathHdfsPath");
            String flinkTemporaryJarPath = systemParams.getString("flinkTemporaryJarPath");

            // sdk包
            String jarName = requestParams.getString("flinkJar");
            String sdkName = jarName.split("/")[jarName.split("/").length - 1];
            String localSdkPath = flinkTemporaryJarPath + File.separator + sdkName;
            File sdkFile = new File(localSdkPath);
            if (!sdkFile.exists()) {
                HadoopClient.downloadFileToLocal(HadoopClient.DEFAULT_NAMENODE, HadoopClient.DEFAULT_USER,
                        flinkSdkHdfsPath + File.separator + sdkName, localSdkPath);
            }
            yarnRunCommand = yarnRunCommand.replaceAll("\\$sdk",
                    localSdkPath.replaceAll("\\\\", "\\\\\\\\"));

            // ck
            if (clientParams.has(FIELD_NAME_SAVEPOINTPATH)) {
                String nonState = "";
                if (clientParams.getBoolean(FIELD_NAME_ALLOWNONRESTOREDSTATE)) {
                    nonState = " -n ";
                }

                yarnRunCommand = yarnRunCommand.replaceAll("\\$savepoint",
                        " -s " + clientParams.getString(FIELD_NAME_SAVEPOINTPATH).replaceAll("\\\\", "\\\\\\\\")
                        + nonState
                );
            } else {
                yarnRunCommand = yarnRunCommand.replaceAll("\\$savepoint", "");
            }

            // 第三方依赖/业务依赖
            List classPaths = requestParams.getJSONArray("classPaths").toList();
            StringBuilder cpString = new StringBuilder();
            for (int i = 0; i < classPaths.size(); i++) {
                String cp = (String) classPaths.get(i);
                String cpName = cp.split("/")[cp.split("/").length - 1];
                String localCpPath = flinkTemporaryJarPath + File.separator + cpName;
                File cpFile = new File(localCpPath);
                if (!cpFile.exists()) {
                    HadoopClient.downloadFileToLocal(HadoopClient.DEFAULT_NAMENODE, HadoopClient.DEFAULT_USER,
                            flinkClasspathHdfsPath + File.separator + cpName, localCpPath);
                }

                cpString.append(" -C ").append("file:").append(File.separator);
                if (isLinux()) {
                    cpString.append(File.separator);
                }
                cpString.append(localCpPath);
            }
            yarnRunCommand = yarnRunCommand.replaceAll("\\$classpath", cpString.toString()
                    .replaceAll("\\\\", "\\\\\\\\"));

            yarnRunCommand = yarnRunCommand.replaceAll("\\$params", clientParams.getString(FIELD_NAME_PROGRAM_ARGUMENTS).trim());
            logger.info("yarn run command: " + yarnRunCommand);

            JSONObject properties = new JSONObject();
            properties.put("HADOOP_USER_NAME", systemParams.getString("flinkHadoopUserName"));

            requestParams = new JSONObject();
            requestParams.put("args", translateCommandline("run " + yarnRunCommand));
            requestParams.put("properties", properties);

            JSONObject callback = CliFrontend.handle(requestParams);
            return callback.toString();
        } else {
            String jobManager = requestParams.getString("jobManager");
            String jarName = requestParams.getString("flinkJar");
            List classPaths = requestParams.getJSONArray("classPaths").toList();
            String url = String.format(SUBMIT_JOB, jobManager, getJarId(jobManager, jarName));
            if (executeType.equals(ExecuteType.EXPLAIN)) {
//            return "curl -H \"Content-Type: application/json\" -XPOST '" + url + "' -d'" + param.toString() + "'";
                JSONObject jo = new JSONObject();
                jo.put("url", url);
                jo.put("classPaths", classPaths);
                jo.put("param", clientParams);
                return jo.toString();
            } else if (executeType.equals(ExecuteType.RESTFUL)) {
                return HttpReqUtil.post(url, RequestBody.create(HttpReqUtil.jsonType, clientParams.toString()), true);
            } else if (executeType.equals(ExecuteType.CLIENT)) {
                ClusterClient<?> clusterClient;
                if (clientMap.containsKey(jobManager)) {
                    clusterClient = clientMap.get(jobManager);
                } else {
                    Configuration config = new Configuration();
                    String[] masterInfo = jobManager.split(":");
                    config.setString("jobmanager.rpc.address", masterInfo[0]);
                    if (masterInfo.length == 2) {
                        config.setString("rest.port", masterInfo[1]);
                    }

                    clusterClient = new FlinkClusterClient(config, StandaloneClusterId.getInstance());
                    // 独立模式，托管给flink
                    // ps: 1.10.0版本没有这方法，默认就是托管
//                clusterClient.setDetached(true);
                    clientMap.put(jobManager, clusterClient);
                }

                String flinkSdkHdfsPath = systemParams.getString("flinkSdkHdfsPath");
                String flinkClasspathHdfsPath = systemParams.getString("flinkClasspathHdfsPath");
                String flinkTemporaryJarPath = systemParams.getString("flinkTemporaryJarPath");

                String sdkName = jarName.split("/")[jarName.split("/").length - 1];
                File sdkFile = new File(flinkTemporaryJarPath + "/" + sdkName);
                if (!sdkFile.exists()) {
                    HadoopClient.downloadFileToLocal(HadoopClient.DEFAULT_NAMENODE, HadoopClient.DEFAULT_USER,
                            flinkSdkHdfsPath + "/" + sdkName, flinkTemporaryJarPath + "/" + sdkName);
                }

                List<URL> urlList = new ArrayList<>();
                for (int i = 0; i < classPaths.size(); i++) {
                    String cp = (String) classPaths.get(i);
                    String cpName = cp.split("/")[cp.split("/").length - 1];
                    File cpFile = new File(flinkTemporaryJarPath + "/" + cpName);
                    if (!cpFile.exists()) {
                        HadoopClient.downloadFileToLocal(HadoopClient.DEFAULT_NAMENODE, HadoopClient.DEFAULT_USER,
                                flinkClasspathHdfsPath + "/" + cpName, flinkTemporaryJarPath + "/" + cpName);
                    }
                    urlList.add(cpFile.toURI().toURL());
                }

                String[] args = clientParams.getString(FIELD_NAME_PROGRAM_ARGUMENTS).trim().split(" ");
                PackagedProgram.Builder builder = PackagedProgram.newBuilder()
                        .setJarFile(sdkFile)
                        .setUserClassPaths(urlList)
                        .setArguments(args);
                if (clientParams.has(FIELD_NAME_SAVEPOINTPATH)) {
                    builder.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(
                            clientParams.getString(FIELD_NAME_SAVEPOINTPATH), clientParams.getBoolean(FIELD_NAME_ALLOWNONRESTOREDSTATE)));
                }
                PackagedProgram prg = builder.build();

                JSONObject jo = new JSONObject();

                try {
                    JobGraph jobGraph = PackagedProgramUtils.createJobGraph(prg, clusterClient.getFlinkConfiguration(),
                            clientParams.getInt(FIELD_NAME_PARALLELISM), false);
                    jobGraph.addJars(urlList);
                    JobSubmissionResult result = ClientUtils.submitJob(clusterClient, jobGraph);
                    jo.put("jobid", result.getJobID().toHexString());
                } catch (Exception e) {
                    final Writer result = new StringWriter();
                    final PrintWriter printWriter = new PrintWriter(result);
                    e.printStackTrace(printWriter);
                    jo.put("errors", result.toString());
                }
                return jo.toString();
            }
        }

        throw new RuntimeException("can not handle execute type");
    }

    public static String detail(String jobManager, String jobId) throws Exception {
        String url = String.format(POINT_JOB, jobManager, jobId);
        return HttpReqUtil.get(url, true);
    }

    public static String terminate(String jobManager, String jobId, boolean execute) throws Exception {
        String url = String.format(POINT_JOB, jobManager, jobId);
        if (!execute) {
            return "curl -H \"Content-Type: application/json\" -XPATCH '" + url + "' -d'{}'";
        }
        return HttpReqUtil.patch(url, RequestBody.create(HttpReqUtil.jsonType, "{}"), true);
    }

    public static String terminateWithSavepoint(String jobManager, String jobId, boolean execute) throws Exception {
        String url = String.format(SAVEPOINT_JOB, jobManager, jobId);
        if (!execute) {
            return "curl -H \"Content-Type: application/json\" -XPOST '" + url + "' -d'{\"cancel-job\": true}'";
        }
        return HttpReqUtil.post(url, RequestBody.create(HttpReqUtil.jsonType, "{\"cancel-job\": true}"), true);
    }

    public static String getHistoryJobStatus(String jobManager, String jobId) throws Exception {
        String url = null;
        if (StringUtils.isNotBlank(jobId)) {
            url = String.format(STATUS_THE_JOB, jobManager, jobId);
        } else {
            url = String.format(STATUS_JOB, jobManager);
        }
        return HttpReqUtil.get(url, true);
    }

    public static String getCurrentJobStatus(String jobManager, String jobId) throws Exception {
        String url = null;
        if (StringUtils.isNotBlank(jobId)) {
            url = String.format(STATUS_THE_JOB, jobManager, jobId);
        } else {
            url = String.format(STATUS_CURRENT_JOB, jobManager);
        }
        return HttpReqUtil.get(url, true);
    }

    public enum ExecuteType {
        RESTFUL, CLIENT, EXPLAIN, YARN
    }

    /**
     * 动态加载jar
     */
    private static class DynamicJarClassLoader extends URLClassLoader {
        private static boolean canCloseJar = false;
        private List<JarURLConnection> cachedJarFiles;

        static {
            // 1.7之后可以直接调用close方法关闭打开的jar，需要判断当前运行的环境是否支持close方法，如果不支持，需要缓存，避免卸载模块后无法删除jar
            try {
                URLClassLoader.class.getMethod("close");
                canCloseJar = true;
            } catch (NoSuchMethodException e) {
            } catch (SecurityException e) {
            }
        }

        public DynamicJarClassLoader(URL[] urls, ClassLoader parent) {
            super(new URL[]{}, parent);
            init(urls);
        }

        public DynamicJarClassLoader(URL[] urls) {
            super(new URL[]{});
            init(urls);
        }

        public DynamicJarClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
            super(new URL[]{}, parent, factory);
            init(urls);
        }

        private void init(URL[] urls) {
            cachedJarFiles = canCloseJar ? null : new ArrayList<JarURLConnection>();
            if (urls != null) {
                for (URL url : urls) {
                    this.addURL(url);
                }
            }
        }

        @Override
        protected void addURL(URL url) {
            if (!canCloseJar) {
                try {
                    // 打开并缓存文件url连接
                    URLConnection uc = url.openConnection();
                    if (uc instanceof JarURLConnection) {
                        uc.setUseCaches(true);
                        ((JarURLConnection) uc).getManifest();
                        cachedJarFiles.add((JarURLConnection) uc);
                    }
                } catch (Exception e) {
                }
            }
            super.addURL(url);
        }

        public void close() throws IOException {
            if (canCloseJar) {
                try {
                    super.close();
                } catch (IOException ioe) {
                }
            } else {
                for (JarURLConnection conn : cachedJarFiles) {
                    conn.getJarFile().close();
                }
                cachedJarFiles.clear();
            }
        }
    }

    public static String[] translateCommandline(String toProcess) {
        if (toProcess == null || toProcess.length() == 0) {
            //no command? no string
            return new String[0];
        }
        // parse with a simple finite state machine

        final int normal = 0;
        final int inQuote = 1;
        final int inDoubleQuote = 2;
        int state = normal;
        final StringTokenizer tok = new StringTokenizer(toProcess, "\"\' ", true);
        final ArrayList<String> result = new ArrayList<String>();
        final StringBuilder current = new StringBuilder();
        boolean lastTokenHasBeenQuoted = false;

        while (tok.hasMoreTokens()) {
            String nextTok = tok.nextToken();
            switch (state) {
                case inQuote:
                    if ("\'".equals(nextTok)) {
                        lastTokenHasBeenQuoted = true;
                        state = normal;
                    } else {
                        current.append(nextTok);
                    }
                    break;
                case inDoubleQuote:
                    if ("\"".equals(nextTok)) {
                        lastTokenHasBeenQuoted = true;
                        state = normal;
                    } else {
                        current.append(nextTok);
                    }
                    break;
                default:
                    if ("\'".equals(nextTok)) {
                        state = inQuote;
                    } else if ("\"".equals(nextTok)) {
                        state = inDoubleQuote;
                    } else if (" ".equals(nextTok)) {
                        if (lastTokenHasBeenQuoted || current.length() != 0) {
                            result.add(current.toString());
                            current.setLength(0);
                        }
                    } else {
                        current.append(nextTok);
                    }
                    lastTokenHasBeenQuoted = false;
                    break;
            }
        }
        if (lastTokenHasBeenQuoted || current.length() != 0) {
            result.add(current.toString());
        }
        if (state == inQuote || state == inDoubleQuote) {
            throw new RuntimeException("unbalanced quotes in " + toProcess);
        }
        return result.toArray(new String[result.size()]);
    }
}
