package org.yewc.test.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class HttpClientHandler extends ChannelInboundHandlerAdapter {

    private Map<String, JSONObject> buffer;

    //接收到数据后调用
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf result = (ByteBuf) msg;
        byte[] resultStr = new byte[result.readableBytes()];
        result.readBytes(resultStr);
        JSONArray ja = new JSONArray("[" + new String(resultStr).replaceAll("}\\{", "},{") + "]");
        result.release();

        for (int i = 0; i < ja.length(); i++) {
            JSONObject jo = ja.getJSONObject(i);
            buffer.put(jo.getString("requestId"), jo);
        }
    }

    //客户端连接服务器后调用，简单的就ctx.writeAndFlush(ByteBuf)，复杂点就自定义编解码器
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        buffer = new ConcurrentHashMap<>();
    }

    //完成时调用
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    //发生异常时调用
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    public CompletableFuture callbackData(String requestId) {
        CompletableFuture future = CompletableFuture.supplyAsync(() -> {
            int count = 0;
            while (!buffer.containsKey(requestId) && count <= 100) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(requestId + "提交的作业结果还没返回，次数" + (count++));
            }

            if (!buffer.containsKey(requestId)) {
                return "{\"msg\": \"response to long, over 10 times\"}";
            }

            return buffer.get(requestId);
        });

        return future;
    }
}
