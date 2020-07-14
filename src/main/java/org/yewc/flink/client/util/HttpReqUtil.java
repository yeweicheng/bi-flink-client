package org.yewc.flink.client.util;

import com.squareup.okhttp.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;


/**
 * http请求工具类
 *
 * @author liuxg
 * @date 2015年5月24日
 * @time 下午8:27:56
 */
public class HttpReqUtil {

    public static final MediaType formType = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");
    public static final MediaType jsonType = MediaType.parse("application/json; charset=utf-8");

    public static final String GET = "GET";
    public static final String POST = "POST";
    public static final String DELETE = "DELETE";
    public static final String PUT = "PUT";
    public static final String PATCH = "PATCH";

    private static final OkHttpClient client = new OkHttpClient();

    public static String get(String url) throws IOException {
        return get(url, false);
    }

    public static String get(String url, boolean ignoreError) throws IOException {
        Request request = new Request.Builder().url(url)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful() && !ignoreError)
            throw new IOException("请求有错：" + response);
        return response.body().string();
    }

    public static String post(String url, RequestBody body) throws IOException {
        return post(url, body, false);
    }

    public static String method(String method, String url, RequestBody body, Map<String, String> header, boolean ignoreError) throws IOException {
        Request.Builder rb = new Request.Builder().url(url);
        if (header != null) {
            for (String key : header.keySet()) {
                rb.addHeader(key, header.get(key));
            }
        }

        Request request = rb.method(method, body).build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful() && !ignoreError)
            throw new IOException("请求有错：" + response);
        return response.body().string();
    }

    public static String post(String url, RequestBody body, boolean ignoreError) throws IOException {
        Request request = new Request.Builder().url(url)
                .post(body).build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful() && !ignoreError)
            throw new IOException("请求有错：" + response);
        return response.body().string();

    }

    public static String patch(String url, RequestBody body) throws IOException {
        return patch(url, body, false);
    }

    public static String patch(String url, RequestBody body, boolean ignoreError) throws IOException {
        Request request = new Request.Builder().url(url).patch(body)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful() && !ignoreError)
            throw new IOException("请求有错：" + response);
        return response.body().string();
    }

    public static class FormData {

        private JSONObject data;

        private FormData() {
        }

        ;

        public static FormData create() {
            FormData fd = new HttpReqUtil.FormData();
            fd.data = new JSONObject();
            return fd;
        }

        public void put(String key, Object value) {
            this.data.put(key, value);
        }

        public String flush() {
            final StringBuilder result = new StringBuilder();
            for (String key : this.data.keySet()) {
                result.append(key).append("=").append(this.data.get(key)).append("&");
            }
            return result.toString();
        }

    }

}

