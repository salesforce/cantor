/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class EventsOnMysql extends AbstractBaseEventsOnJdbc implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnMysql.class);

    public EventsOnMysql(final String hostname, final int port, final String username, final String password)
            throws IOException {
        this(MysqlDataSourceProvider.getDatasource(
                new MysqlDataSourceProperties()
                        .setHostname(hostname)
                        .setPort(port)
                        .setUsername(username)
                        .setPassword(password)
                )
        );
    }

    public EventsOnMysql(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateChunkLookupTableSql(final String namespace) {
        // sql to create chunk lookup table
        return String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        " %s VARCHAR(256) NOT NULL," +  // table name
                        " %s VARCHAR(256) NOT NULL," +  // column name (column name for the key)
                        " %s VARCHAR(256)," +  // key name (name given by user)
                        " %s BIGINT," +  // chunk table start timestamp
                        " PRIMARY KEY (%s, %s), " +  // primary key is table name + column name
                        " INDEX %s (%s)," +  // index column names
                        " INDEX %s (%s)," +  // index key names
                        " INDEX %s (%s) )" +  // index start timestamps
                        " ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName()),
                quote(getColumnColumnName()),
                quote(getKeyColumnName()),
                quote(getStartTimestampMillisColumnName()),
                quote(getTableNameColumnName()), quote(getColumnColumnName()),
                quote(getKeyColumnName()), quote(getKeyColumnName()),
                quote(getColumnColumnName()), quote(getColumnColumnName()),
                quote(getStartTimestampMillisColumnName()), quote(getStartTimestampMillisColumnName())
        );
    }

    @Override
    protected String getCreateChunkTableSql(final String chunkTableName,
                                            final String namespace,
                                            final Map<String, String> metadata,
                                            final Map<String, Double> dimensions) {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE IF NOT EXISTS ")
                .append(getTableFullName(namespace, chunkTableName)).append(" (")
                .append(quote(getEventTimestampColumnName())).append(" BIGINT, ");
        for (final String metadataKey : getOrderedKeys(metadata)) {
            sqlBuilder.append(quote(getMetadataKeyColumnName(metadataKey))).append(" TEXT, ");
        }
        for (final String dimensionKey : getOrderedKeys(dimensions)) {
            sqlBuilder.append(quote(getDimensionKeyColumnName(dimensionKey))).append(" DOUBLE, ");
        }
        sqlBuilder.append(quote(getPayloadColumnName())).append(" LONGBLOB, ")
                .append("INDEX ").append(getEventTimestampColumnName())
                .append(" (").append(quote(getEventTimestampColumnName())).append(") ");

        final int maxIndexedColumns = 63;
        int indexedColumnsCount = 0;
        // add index to as many metadata as possible
        for (final String metadataKey : getOrderedKeys(metadata)) {
            if (++indexedColumnsCount > maxIndexedColumns) {
                // only so many columns can be indexed
                break;
            }
            sqlBuilder.append(", INDEX ")
                    .append(quote(getMetadataKeyColumnName(metadataKey)))
                    .append(" (").append(quote(getMetadataKeyColumnName(metadataKey))).append("(256)) ");
        }
        // add index for dimensions if possible
        for (final String dimensionKey : getOrderedKeys(dimensions)) {
            if (++indexedColumnsCount > maxIndexedColumns) {
                // only so many columns can be indexed
                break;
            }
            sqlBuilder.append(", INDEX ")
                    .append(quote(getDimensionKeyColumnName(dimensionKey)))
                    .append(" (").append(quote(getDimensionKeyColumnName(dimensionKey))).append(") ");
        }

        sqlBuilder.append(")");
        sqlBuilder.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8 ");
        return sqlBuilder.toString();
    }

    @Override
    protected String getRegexQuery(final String column) {
        return String.format(" %s REGEXP ? ", column);
    }

    @Override
    protected String getNotRegexQuery(final String column) {
        return String.format(" %s NOT REGEXP ? ", column);
    }
}

