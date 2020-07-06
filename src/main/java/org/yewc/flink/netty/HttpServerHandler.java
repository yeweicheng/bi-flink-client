package org.yewc.flink.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.flink.client.util.FlinkUtil;
import org.yewc.flink.yarn.CliFrontend;

import java.util.List;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {

    public static final Logger logger = LoggerFactory.getLogger(HttpServerHandler.class);

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
                    case "list":
                        list(ctx, jo);
                        break;
                    case "cancel":
                        cancel(ctx, jo);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();

                JSONObject callback = new JSONObject();
                callback.put("requestId", requestId);
                callback.put("msg", e.getMessage());
                String data = callback.toString();
                logger.info("callback{" + (4 * data.length()) + ") -> " + data);

                ByteBuf encoded = ctx.alloc().buffer(4 * data.length());
                encoded.writeBytes(data.getBytes());
                ctx.write(encoded);
                ctx.flush();
            }
        }
    }

    private void submit(ChannelHandlerContext ctx, JSONObject jo) throws Exception {
        // 向客户端发送消息
        logger.info("request -> " + jo.toString());

        String requestId = jo.getString("requestId");
        String type = jo.getString("executeType");
        FlinkUtil.ExecuteType executeType = null;
        if ("client".equals(type)) {
            executeType = FlinkUtil.ExecuteType.CLIENT;
        } else if ("restful".equals(type)) {
            executeType = FlinkUtil.ExecuteType.RESTFUL;
        } else if ("explain".equals(type)) {
            executeType = FlinkUtil.ExecuteType.EXPLAIN;
        } else if ("yarn".equals(type)) {
            executeType = FlinkUtil.ExecuteType.YARN;
        }

        String returnData = FlinkUtil.submit(jo, executeType);
        JSONObject callback = new JSONObject(returnData);
        callback.put("requestId", requestId);

        flushToClient(ctx, callback.toString());
    }

    private void list(ChannelHandlerContext ctx, JSONObject jo) throws Exception {
        // 向客户端发送消息
        logger.info("request -> " + jo.toString());

        String requestId = jo.getString("requestId");

        JSONObject params = new JSONObject();
        StringBuilder args = new StringBuilder("list");
        if (jo.has("yid")) {
            args.append(" -yid ").append(jo.getString("yid"));
        }
        params.put("args", args.toString().split("\\s+"));

        JSONObject callback = CliFrontend.handle(params);
        callback.put("requestId", requestId);

        flushToClient(ctx, callback.toString());
    }

    private void cancel(ChannelHandlerContext ctx, JSONObject jo) throws Exception {
        // 向客户端发送消息
        logger.info("request -> " + jo.toString());

        String requestId = jo.getString("requestId");

        JSONObject params = new JSONObject();
        StringBuilder args = new StringBuilder("cancel");
        if (jo.has("savepoint")) {
            args.append(" -s ").append(jo.getString("savepoint"));
        }
        args.append(" -yid ").append(jo.getString("yid")).append(" ").append(jo.getString("fid"));
        params.put("args", args.toString().split("\\s+"));

        JSONObject callback = CliFrontend.handle(params);
        callback.put("requestId", requestId);

        flushToClient(ctx, callback.toString());
    }

    private void flushToClient(ChannelHandlerContext ctx, String data) {
        logger.info("callback{" + (4 * data.length()) + ") -> " + data);
        ByteBuf encoded = ctx.alloc().buffer(4 * data.length());
        encoded.writeBytes(data.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("远程有连接进入");
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
        logger.info("远程有连接退出");
        super.channelInactive(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error(cause.getMessage());
//        super.exceptionCaught(ctx, cause);
    }
}
