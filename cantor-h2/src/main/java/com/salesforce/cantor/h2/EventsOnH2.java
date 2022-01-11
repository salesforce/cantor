/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class EventsOnH2 extends AbstractBaseEventsOnJdbc implements Events {

    public EventsOnH2(final String path) throws IOException {
        this(H2DataSourceProvider.getDatasource(new H2DataSourceProperties().setPath(path)));
    }

    public EventsOnH2(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateInternalDatabaseSql() {
        return H2Utils.getH2CreateDatabaseSql(getCantorInternalDatabaseName());
    }

    @Override
    protected String getCreateDatabaseSql(final String namespace) {
        return H2Utils.getH2CreateDatabaseSql(namespace);
    }

    @Override
    protected String getDropDatabaseSql(final String namespace) {
        return H2Utils.getH2DropDatabaseSql(namespace);
    }

    @Override
    protected String getCreateChunkLookupTableSql(final String namespace) {
        // sql to create chunk lookup table
        return String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        " `ID` INT NOT NULL AUTO_INCREMENT," +  // auto-increment primary key
                        " %s VARCHAR(256)," +  // table name
                        " %s VARCHAR(256)," +  // key name (name given by user)
                        " %s VARCHAR(256)," +  // column name
                        " %s BIGINT," +  // chunk start timestamp
                        " PRIMARY KEY (ID), " +  // primary key is id
                        " INDEX (%s)," +  // index table name
                        " INDEX (%s)," +  // index key name
                        " INDEX (%s)," +  // index column name
                        " INDEX (%s) )",   // index start timestamp
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName()),
                quote(getKeyColumnName()),
                quote(getColumnColumnName()),
                quote(getStartTimestampMillisColumnName()),
                quote(getTableNameColumnName()),
                quote(getKeyColumnName()),
                quote(getColumnColumnName()),
                quote(getStartTimestampMillisColumnName())
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
                .append(quote(getEventTimestampColumnName())).append(" BIGINT,");
        for (final String metadataKey : getOrderedKeys(metadata)) {
            sqlBuilder.append(quote(getMetadataKeyColumnName(metadataKey))).append(" VARCHAR, ");
        }
        for (final String dimensionKey : getOrderedKeys(dimensions)) {
            sqlBuilder.append(quote(getDimensionKeyColumnName(dimensionKey))).append(" DOUBLE, ");
        }
        sqlBuilder.append(quote(getPayloadColumnName())).append(" BLOB,");
        sqlBuilder.append("INDEX ").append("(").append(quote(getEventTimestampColumnName())).append("),");
        for (final String metadataKey : getOrderedKeys(metadata)) {
            sqlBuilder.append("INDEX ").append("(").append(quote(getMetadataKeyColumnName(metadataKey))).append("),");
        }
        for (final String dimensionKey : getOrderedKeys(dimensions)) {
            sqlBuilder.append("INDEX ").append("(").append(quote(getDimensionKeyColumnName(dimensionKey))).append("),");
        }
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length());
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    @Override
    protected String getRegexPattern(final String originalPattern) {
        String finalPattern = originalPattern.replaceAll("\\*", "\\.\\*");

        // To match a prefix, regex pattern starts with "^"
        if (!originalPattern.startsWith("*")) {
            finalPattern = "^" + finalPattern;
        }

        // To match a postfix, regex pattern ends with "$"
        if (!originalPattern.endsWith("*")) {
            finalPattern = finalPattern + "$";
        }

        return finalPattern;
    }

    @Override
    protected String getRegexQuery(final String column) {
        return String.format(" REGEXP_LIKE (%s, ?) ", column);
    }

    @Override
    protected String getNotRegexQuery(final String column) {
        return String.format(" NOT REGEXP_LIKE (%s, ?) ", column);
    }
}

