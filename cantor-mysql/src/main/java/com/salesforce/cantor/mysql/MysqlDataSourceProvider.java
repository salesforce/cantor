/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class MysqlDataSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(MysqlDataSourceProvider.class);

    public static synchronized DataSource getDatasource(final MysqlDataSourceProperties builder) {
        return doGetDataSource(
                builder.getHostname(),
                builder.getPort(),
                builder.getUsername(),
                builder.getPassword()
        );
    }

    private static DataSource doGetDataSource(final String hostname,
                                              final int port,
                                              final String username,
                                              final String password) {
        final String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/";
        try {
            // force loading mysql driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        final HikariDataSource connectoinPoolDataSource = new HikariDataSource();

        connectoinPoolDataSource.setJdbcUrl(jdbcUrl);
        connectoinPoolDataSource.setUsername(username);
        if (password != null) {
            connectoinPoolDataSource.setPassword(password);
        }

        connectoinPoolDataSource.setConnectionTimeout(30 * 1000);  // 30 seconds connection timeout
        connectoinPoolDataSource.addDataSourceProperty("useUnicode", true);
        connectoinPoolDataSource.addDataSourceProperty("useSSL", false);
        connectoinPoolDataSource.addDataSourceProperty("allowPublicKeyRetrieval", true);
        connectoinPoolDataSource.addDataSourceProperty("serverTimezone", "UTC");
        connectoinPoolDataSource.addDataSourceProperty("useJDBCCompliantTimezoneShift", true);
        connectoinPoolDataSource.addDataSourceProperty("useLegacyDatetimeCode", false);
        connectoinPoolDataSource.addDataSourceProperty("cachePrepStmts", true);
        connectoinPoolDataSource.addDataSourceProperty("prepStmtCacheSize", 512);
        connectoinPoolDataSource.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        connectoinPoolDataSource.addDataSourceProperty("useServerPrepStmts", true);
        connectoinPoolDataSource.addDataSourceProperty("useLocalSessionState", true);
        connectoinPoolDataSource.addDataSourceProperty("rewriteBatchedStatements", true);
        connectoinPoolDataSource.addDataSourceProperty("cacheResultSetMetadata", true);
        connectoinPoolDataSource.addDataSourceProperty("metadataCacheSize", 512);
        connectoinPoolDataSource.addDataSourceProperty("cacheServerConfiguration", true);
        connectoinPoolDataSource.addDataSourceProperty("elideSetAutoCommits", true);
        connectoinPoolDataSource.addDataSourceProperty("continueBatchOnError", false);
        connectoinPoolDataSource.addDataSourceProperty("maintainTimeStats", false);
        connectoinPoolDataSource.addDataSourceProperty("maxRows", 100_000);  // max of 100,000 rows to be returned
//        TODO allow users to override this by a flag
//        connectoinPoolDataSource.addDataSourceProperty("logger", "com.mysql.cj.log.Slf4JLogger");
//        connectoinPoolDataSource.addDataSourceProperty("profileSQL", true);

        connectoinPoolDataSource.setMaximumPoolSize(64);

        logger.info("jdbc url for datasource is: {} data source properties are: {}",
                jdbcUrl, connectoinPoolDataSource.getDataSourceProperties()
        );
        return connectoinPoolDataSource;
    }
}

