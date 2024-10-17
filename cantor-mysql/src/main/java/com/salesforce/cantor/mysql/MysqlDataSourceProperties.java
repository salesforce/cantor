/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

public class MysqlDataSourceProperties {

    private String hostname = "localhost";
    private int port = 3306;
    private String username = "root";
    private String password = null;

    public int getPort() {
        return this.port;
    }

    public MysqlDataSourceProperties setPort(final int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return this.username;
    }

    public MysqlDataSourceProperties setUsername(final String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return this.password;
    }

    public MysqlDataSourceProperties setPassword(final String password) {
        this.password = password;
        return this;
    }

    public String getHostname() {
        return this.hostname;
    }

    public MysqlDataSourceProperties setHostname(final String hostname) {
        this.hostname = hostname;
        return this;
    }

    @Override
    public String toString() {
        return "MysqlDataSourceProperties(" + getHostname() + ":" + getPort() + ")";
    }
}

