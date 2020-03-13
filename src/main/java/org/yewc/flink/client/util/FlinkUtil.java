package org.yewc.flink.client.util;

import com.squareup.okhttp.RequestBody;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static String submit(String jobManager, String jarName, List<String> classPaths,
                                JSONObject clientParams, JSONObject systemParams, ExecuteType executeType) throws Exception {
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
                clusterClient.setDetached(true);
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
                String cp = classPaths.get(i);
                String cpName = cp.split("/")[cp.split("/").length - 1];
                File cpFile = new File(flinkTemporaryJarPath + "/" + cpName);
                if (!cpFile.exists()) {
                    HadoopClient.downloadFileToLocal(HadoopClient.DEFAULT_NAMENODE, HadoopClient.DEFAULT_USER,
                            flinkClasspathHdfsPath + "/" + cpName, flinkTemporaryJarPath + "/" + cpName);
                }
                urlList.add(cpFile.toURI().toURL());
            }

            String[] args = clientParams.getString(FIELD_NAME_PROGRAM_ARGUMENTS).trim().split(" ");
            PackagedProgram prg = new PackagedProgram(sdkFile, urlList, args);
            if (clientParams.has(FIELD_NAME_SAVEPOINTPATH)) {
                prg.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(clientParams.getString(FIELD_NAME_SAVEPOINTPATH)));
            }

            JSONObject jo = new JSONObject();

            try {
//                DynamicJarClassLoader classLoader = null;
//                if (urlList.size() > 0) {
//                    ClassLoader cl = Thread.class.getClassLoader();
//                    classLoader = new DynamicJarClassLoader(urlList.toArray(new URL[urlList.size()]), cl);
//                }
                JobSubmissionResult result = clusterClient.run(prg, clientParams.getInt(FIELD_NAME_PARALLELISM));
                jo.put("jobid", result.getJobID().toHexString());
//                if (classLoader != null) {
//                    classLoader.close();
//                }
            } catch (Exception e) {
                logger.error("submit failed!", e);

                final Writer result = new StringWriter();
                final PrintWriter printWriter = new PrintWriter(result);
                e.printStackTrace(printWriter);
                jo.put("errors", result.toString());
            }
            return jo.toString();
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
        RESTFUL, CLIENT, EXPLAIN
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

}
