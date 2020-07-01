package com.salesforce.cantor.phoenix;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public class PhoenixDataSourceProperties {
    private String path;
    private String hostname = "localhost";
    private int port = 2181;

    public String getPath() {
        return this.path;
    }

    public PhoenixDataSourceProperties setPath(final String path) {
        checkString(path, "null/empty path");
        this.path = path;
        return this;
    }

    public int getPort() {
        return this.port;
    }

    public PhoenixDataSourceProperties setPort(final int port) {
        this.port = port;
        return this;
    }

    public String getHostname() {
        return this.hostname;
    }

    public PhoenixDataSourceProperties setHostname(final String hostname) {
        this.hostname = hostname;
        return this;
    }

    @Override
    public String toString() {
        return "MysqlDataSourceProperties(" + getHostname() + ":" + getPort() + ")";
    }
}
