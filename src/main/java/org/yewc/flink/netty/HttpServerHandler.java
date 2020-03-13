package org.yewc.flink.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.yewc.flink.client.util.FlinkUtil;

import java.util.List;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {

    //读取客户端发过来的请求，并且向客户端响应
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf theMsg = (ByteBuf) msg;
        byte[] result1 = new byte[theMsg.readableBytes()];
        theMsg.readBytes(result1);
        String resultStr = new String(result1);
        JSONArray ja = new JSONArray("[" + resultStr.replaceAll("}\\{", "},{") + "]");

        for (int i = 0; i < ja.length(); i++) {
            JSONObject jo = ja.getJSONObject(i);
            String requestId = jo.getString("requestId");
            String requestType = jo.getString("requestType");
            try {
                switch (requestType) {
                    case "submit":
                        submit(ctx, jo);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();

                JSONObject callback = new JSONObject();
                callback.put("requestId", requestId);
                callback.put("msg", e.getMessage());
                String data = callback.toString();

                ByteBuf encoded = ctx.alloc().buffer(4 * data.length());
                encoded.writeBytes(data.getBytes());
                ctx.write(encoded);
                ctx.flush();
            }
        }
    }

    private void submit(ChannelHandlerContext ctx, JSONObject jo) throws Exception {
        // 向客户端发送消息
        System.out.println("request -> " + jo.toString());

        String requestId = jo.getString("requestId");
        String jobManager = jo.getString("jobManager");
        String flinkJar = jo.getString("flinkJar");
        List classPaths = jo.getJSONArray("classPaths").toList();
        JSONObject params = jo.getJSONObject("params");
        String type = jo.getString("executeType");
        FlinkUtil.ExecuteType executeType = null;
        if ("client".equals(type)) {
            executeType = FlinkUtil.ExecuteType.CLIENT;
        } else if ("restful".equals(type)) {
            executeType = FlinkUtil.ExecuteType.RESTFUL;
        } else if ("explain".equals(type)) {
            executeType = FlinkUtil.ExecuteType.EXPLAIN;
        }

        String returnData = FlinkUtil.submit(jobManager, flinkJar, classPaths, params, executeType);
        JSONObject callback = new JSONObject(returnData);
        callback.put("requestId", requestId);

        String data = callback.toString();
        System.out.println("callback -> " + data);

        ByteBuf encoded = ctx.alloc().buffer(4 * data.length());
        encoded.writeBytes(data.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("远程有连接进入");
        super.channelActive(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("远程有连接退出");
        super.channelInactive(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println(cause.getMessage());
//        super.exceptionCaught(ctx, cause);
    }
}
