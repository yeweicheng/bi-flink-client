package org.yewc.flink.client.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.squareup.okhttp.RequestBody;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.ArrayList;
import java.util.List;

public class PushGatewayHandler {

    private static YarnClient client;
    private static String prometheusUrl;
    private static String pushgatewayUrl;

    public static void init(String prometheusUrl, String pushgatewayUrl) throws Exception {
        if (client == null) {
            YarnConfiguration configuration = new YarnConfiguration();
            client = YarnClient.createYarnClient();
            client.init(configuration);
            client.start();
        }

        PushGatewayHandler.prometheusUrl = prometheusUrl;
        PushGatewayHandler.pushgatewayUrl = pushgatewayUrl;
    }

    public static void stop() throws Exception {
        if (client != null) {
            client.stop();
        }
    }

    public static void clearJmMetrics() throws Exception {
        String url = "http://" + prometheusUrl + "/api/v1/query?query=flink_jobmanager_Status_JVM_CPU_Load";
        String content = HttpReqUtil.get(url);

        JSONObject jo = JSONObject.parseObject(content);
        if (!jo.containsKey("status") || !"success".equals(jo.getString("status"))) {
            throw new RuntimeException("Fail to get prometheus metrics!");
        }

        JSONArray result = jo.getJSONObject("data").getJSONArray("result");
        for (int i = 0; i < result.size(); i++) {
            JSONObject metric = result.getJSONObject(i).getJSONObject("metric");
            if (metric.getString("job").startsWith("bi-flink")) {
                HttpReqUtil.method(HttpReqUtil.DELETE, "http://" + pushgatewayUrl + "/metrics/job/" + metric.getString("job"),
                        RequestBody.create(HttpReqUtil.jsonType, "{}"), null, false);
            }
        }
    }

    public static void clearTmMetrics() throws Exception {
        String url = "http://" + prometheusUrl + "/api/v1/query?query=flink_taskmanager_Status_JVM_CPU_Load";
        String content = HttpReqUtil.get(url);

        JSONObject jo = JSONObject.parseObject(content);
        if (!jo.containsKey("status") || !"success".equals(jo.getString("status"))) {
            throw new RuntimeException("Fail to get prometheus metrics!");
        }

        JSONObject containerMap = new JSONObject();
        JSONArray result = jo.getJSONObject("data").getJSONArray("result");
        for (int i = 0; i < result.size(); i++) {
            JSONObject metric = result.getJSONObject(i).getJSONObject("metric");
            if (metric.getString("job").startsWith("bi-flink")) {
                containerMap.put(metric.getString("tm_id"), metric.getString("job"));
            }
        }

        List<String> attempIds = new ArrayList<>();
        for (String key : containerMap.keySet()) {
            String[] temp = key.split("_");
            attempIds.add("appattempt_" + temp[2] + "_" + temp[3] + "_" + "0000" + temp[4]);
        }

        JSONObject containerStatus = getContainerStatus(attempIds);
        for (String aid : containerStatus.keySet()) {
            JSONObject csJo = containerStatus.getJSONObject(aid);
            for (String cid : csJo.keySet()) {
                if ("COMPLETE".equals(csJo.getString(cid))) {
                    HttpReqUtil.method(HttpReqUtil.DELETE, "http://" + pushgatewayUrl + "/metrics/job/" + containerMap.get(cid),
                            RequestBody.create(HttpReqUtil.jsonType, "{}"), null, false);
                }
            }
        }
    }

    public static JSONObject getContainerStatus(List<String> attempIds) throws Exception {
        JSONObject atMap = new JSONObject();
        for (int i = 0; i < attempIds.size(); i++) {
            List<ContainerReport> crList = client.getContainers(ConverterUtils
                    .toApplicationAttemptId(attempIds.get(i)));
            JSONObject csMap = new JSONObject();
            for (int j = 0; j < crList.size(); j++) {
                ContainerReport cr = crList.get(j);
                csMap.put(cr.getContainerId().toString(), cr.getContainerState().name());
            }

            atMap.put(attempIds.get(i), csMap);
        }
        return atMap;
    }

    public static void main(String[] args) throws Exception {
        PushGatewayHandler.init("10.16.6.191:9090", "10.16.6.191:9091");
        PushGatewayHandler.clearJmMetrics();
        PushGatewayHandler.clearTmMetrics();
        PushGatewayHandler.stop();
    }
}
