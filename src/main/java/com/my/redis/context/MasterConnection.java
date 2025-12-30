package com.my.redis.context;

import java.net.InetSocketAddress;

public class MasterConnection {

    private final int port;
    private final String host;

    public MasterConnection(String url) {
        String[] split = url.split(" ");

        this.host = split[0];
        this.port = Integer.parseInt(split[1]);
    }


    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
    }
}
