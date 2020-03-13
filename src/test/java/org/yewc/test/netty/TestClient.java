package org.yewc.test.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.json.JSONObject;

public class TestClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "22222"));
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public static void start(final String name) throws Exception {
        System.out.println("EchoClient.main " + name);

        // Configure the client.
        /*创建一个Bootstrap b实例用来配置启动客户端
         * b.group指定NioEventLoopGroup来处理连接，接收数据
         * b.channel指定通道类型
         * b.option配置参数
         * b.handler客户端成功连接服务器后就会执行
         * b.connect客户端连接服务器
         * b.sync阻塞配置完成并启动
         */
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            TestHttpClientHandler handler = new TestHttpClientHandler();
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    //.option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(handler);
                        }
                    });

            // Start the client.
            ChannelFuture cf = b.connect(HOST, PORT).sync();
            Channel channel = cf.channel();
            for (int i = 0; i < 10; i++) {
                JSONObject jo = new JSONObject();
                String msg = name + " hello Server! + " + i;
                jo.put("index", i);
                jo.put("msg", msg);

                ByteBuf encoded = channel.alloc().buffer(4 * jo.toString().length());
                encoded.writeBytes(jo.toString().getBytes());
                ChannelFuture askCf = channel.writeAndFlush(encoded).sync();
                askCf.addListener((result) -> {
                    if (result.isSuccess()) { // 是否成功
                        System.out.println(handler.buffer.size());
                    } else {
                        System.out.println("write操作失败，" + result.cause().getMessage());
                    }
                });
            }

            // Wait until the connection is closed.
            channel.closeFuture().sync();

        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    TestClient.start("first");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

}
