package org.yewc.test.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.json.JSONArray;
import org.json.JSONObject;
import org.yewc.test.netty.TestHttpClientHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FlinkNettyClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "22222"));
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public static void start() throws Exception {
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
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    //.option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 2, 0, 2));
                            pipeline.addLast(new LengthFieldPrepender(2));
                            pipeline.addLast(new HttpClientHandler());
                        }
                    });

            // Start the client.
            ChannelFuture cf = b.connect(HOST, PORT).sync();
            Channel channel = cf.channel();

            JSONObject jo = new JSONObject();
            jo.put("requestId", "123456");
            jo.put("requestType", "submit");
            jo.put("jobManager", "10.17.6.146:7081");
            jo.put("flinkJar", "flink-executor-test.jar");
            jo.put("classPaths", new JSONArray());
            jo.put("clientParams", new JSONObject("{\"programArgs\":\" --template-path hdfs://10.16.6.185:8020/flink/job-template/2c938a6e70a4f7fa0170a4fa56810000_1584009005309.json\",\"parallelism\":1}"));
            JSONObject systemJo = new JSONObject();
            systemJo.put("flinkSdkHdfsPath", "/flink/sdk");
            systemJo.put("flinkClasspathHdfsPath", "/flink/classpath");
            systemJo.put("flinkTemporaryJarPath", "e:\\tmp");
            jo.put("systemParams", systemJo);

            jo.put("executeType", "client");

            ByteBuf encoded = channel.alloc().buffer(4 * jo.toString().length());
            encoded.writeBytes(jo.toString().getBytes());
            ChannelFuture askCf = null;
            try {
                askCf = channel.writeAndFlush(encoded).sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            askCf.addListener((result) -> {
                if (result.isSuccess()) { // 是否成功
                    System.out.println("write操作成功");
                    HttpClientHandler handler = (HttpClientHandler) channel.pipeline().last();
                    handler.callbackData(jo.getString("requestId")).whenComplete((v, t) -> {
                        System.out.println(v);
                    });
                } else {
                    System.out.println("write操作失败，" + result.cause().getMessage());
                }
            });
            // Wait until the connection is closed.
            channel.closeFuture().sync();
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        FlinkNettyClient.start();
    }

}
