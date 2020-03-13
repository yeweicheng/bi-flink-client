package org.yewc.flink.entry;

import org.yewc.flink.netty.FlinkNettyServer;

public class ClientMain {

    public static void main(String[] args) throws Exception {
//        int port = Integer.valueOf(args[0]);
        int port = 22223;
        FlinkNettyServer.start(port);
    }

}
