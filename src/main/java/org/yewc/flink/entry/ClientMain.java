package org.yewc.flink.entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.flink.client.util.PushGatewayHandler;
import org.yewc.flink.netty.FlinkNettyServer;

public class ClientMain {

    public static final Logger logger = LoggerFactory.getLogger(ClientMain.class);

    public static void main(String[] args) throws Exception {
//        args = new String[]{"22223", "10.16.6.191:9090", "10.16.6.191:9091"};

        clearMetrics(args);
        int port = Integer.valueOf(args[0]);
        FlinkNettyServer.start(port);
    }

    private static void clearMetrics(String[] args) throws Exception {
        if (args.length < 3) {
            return;
        }

        PushGatewayHandler.init(args[1], args[2]);

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(3600000);
                    PushGatewayHandler.clearJmMetrics();
                } catch (Exception e) {
                    logger.error("clearJmMetrics", e);
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60000);
                    PushGatewayHandler.clearTmMetrics();
                } catch (Exception e) {
                    logger.error("clearTmMetrics", e);
                }
            }
        }).start();
    }
}
