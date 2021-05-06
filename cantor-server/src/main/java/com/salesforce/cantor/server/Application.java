/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server;

import com.salesforce.cantor.server.grpc.GrpcServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            printUsage();
            return;
        }

        // set system wide default timezone to utc
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        final String configPath = args[0];
        logger.info("loading configs from {}", configPath);
        final Config cantorProperties = ConfigFactory.parseFile(new File(configPath)).resolve();
        logger.info("configs are: {}", cantorProperties);
        final CantorEnvironment environment = new CantorEnvironment(cantorProperties);

        printCantor();
        if (environment.getConfigAsInteger(Constants.CANTOR_PORT_GRPC, -1) != -1) {
            logger.info("'cantor.grpc.port' is set. setting up grpc server...");
            final GrpcServer grpcServer = new GrpcServer(environment);
            grpcServer.start().join();
        } else {
            logger.info("'cantor.grpc.port' not set. will not attempt to set up grpc server.");
        }

        // redirect JUL to slf4j
        SLF4JBridgeHandler.install();
    }

    private static void printUsage() {
        System.err.println("usage: java -jar cantor-server.jar <path-to-config>");
    }

    private static void printCantor() {
        logger.info("\n\n--- starting cantor ---\n\n");
    }
}
