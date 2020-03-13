package org.yewc.test.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.json.JSONArray;
import org.json.JSONObject;

public class TestHttpServerHandler extends ChannelInboundHandlerAdapter {

    //读取客户端发过来的请求，并且向客户端响应
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
//        System.out.println("Client said:" + resultStr);
        JSONArray ja = new JSONArray("[" + resultStr.replaceAll("}\\{", "},{") + "]");
        // 接收并打印客户端的信息
        System.out.println("Client said:" + ja.toString());
        // 释放资源，这行很关键
        result.release();

        for (int i = 0; i < ja.length(); i++) {
            // 向客户端发送消息
            String index = ja.getJSONObject(i).get("index").toString();
            String response = "hello client! + " + index;
            JSONObject jo = new JSONObject();
            jo.put("index", index);
            jo.put("msg", response);

            // 在当前场景下，发送的数据必须转换成ByteBuf数组
            ByteBuf encoded = ctx.alloc().buffer(4 * jo.toString().length());
            encoded.writeBytes(jo.toString().getBytes());
            ctx.write(encoded);
            ctx.flush();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channel active");
        super.channelActive(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channel registered");
        super.channelRegistered(ctx);
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        System.out.println("handler added");
        super.handlerAdded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channel inactive");
        super.channelInactive(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channel unregister");
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println(cause.getMessage());
//        super.exceptionCaught(ctx, cause);
    }
}
