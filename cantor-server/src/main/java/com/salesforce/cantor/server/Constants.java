/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server;

/**
 * Server configuration constants
 */
public class Constants {
    // config path
    public static final String PATH_CONFIG_FILE = "cantor-server.conf";

    static final String CONFIG_ROOT_PREFIX = "cantor";

    // general configurations
    public static final String CANTOR_STORAGE_TYPE = "storage.type";
    public static final String CANTOR_PORT_GRPC = "grpc.port";

    // h2 configurations
    public static final String CANTOR_H2_PATH = "path";
    public static final String CANTOR_H2_IN_MEMORY = "in-memory";
    public static final String CANTOR_H2_COMPRESSED = "compressed";
    public static final String CANTOR_H2_USERNAME = "username";
    public static final String CANTOR_H2_PASSWORD = "password";

    // mysql configurations
    public static final String CANTOR_MYSQL_HOSTNAME = "hostname";
    public static final String CANTOR_MYSQL_PORT = "port";
    public static final String CANTOR_MYSQL_USERNAME = "username";
    public static final String CANTOR_MYSQL_PASSWORD = "password";
}
