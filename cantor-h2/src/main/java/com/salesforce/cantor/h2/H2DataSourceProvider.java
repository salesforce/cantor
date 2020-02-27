/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class H2DataSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(H2DataSourceProvider.class);
    private static final Map<String, DataSource> datasourceCache = new ConcurrentHashMap<>();

    public static synchronized DataSource getDatasource(final H2DataSourceProperties builder) {
        final String datasourceCacheKey = builder.getPath();
        if (!datasourceCache.containsKey(datasourceCacheKey)) {
            datasourceCache.put(datasourceCacheKey, doGetDataSource(builder));
        }
        return datasourceCache.get(datasourceCacheKey);
    }

    private static DataSource doGetDataSource(final H2DataSourceProperties datasourceProperties) {
        // database file name on disk is "<path>/cantor.db"
        final Path dbPath = Paths.get(String.format("%s/cantor", datasourceProperties.getPath()));

        final String jdbcUrl = String.format(
                "jdbc:h2:%s:%s;" +
                        "MODE=MYSQL;" +
                        "COMPRESS=" + String.valueOf(datasourceProperties.isCompressed()).toUpperCase() + ";" +
                        "LOCK_TIMEOUT=30000;" +
                        "DB_CLOSE_ON_EXIT=FALSE;" +
                        "TRACE_LEVEL_FILE=1;" +
                        "TRACE_MAX_FILE_SIZE=4;" +
                        "AUTOCOMMIT=TRUE;" +
                        "AUTO_SERVER=" + String.valueOf(datasourceProperties.isAutoServer()).toUpperCase() + ";" +
                        "LOCK_MODE=1;" +
                        "MAX_COMPACT_TIME=3000;",
                (datasourceProperties.isInMemory() ? "mem" : "split"),
                dbPath.toAbsolutePath().toString());
        logger.info("jdbc url for datasource is: {}", jdbcUrl);

        try {
            // force loading h2 driver
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        final HikariDataSource connectoinPoolDataSource = new HikariDataSource();
        connectoinPoolDataSource.setJdbcUrl(jdbcUrl);
        connectoinPoolDataSource.setUsername(datasourceProperties.getUsername());
        connectoinPoolDataSource.setPassword(datasourceProperties.getPassword());
        connectoinPoolDataSource.setMaximumPoolSize(datasourceProperties.getMaxPoolSize());
        connectoinPoolDataSource.setConnectionTimeout(datasourceProperties.getConnectionTimeoutMillis());

        return connectoinPoolDataSource;
    }
}

