/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.salesforce.cantor.Maps;
import com.salesforce.cantor.jdbc.AbstractBaseMapsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class MapsOnMysql extends AbstractBaseMapsOnJdbc implements Maps {

    public MapsOnMysql(final String hostname, final int port, final String username, final String password)
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

    public MapsOnMysql(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateLookupTableSql(final String namespace) {
        // sql to create chunk lookup table
        return String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        " %s VARCHAR(256) NOT NULL," +  // table name
                        " %s VARCHAR(256) NOT NULL," +  // column name (column name for the key)
                        " %s VARCHAR(256)," +  // key name (name given by user)
                        " PRIMARY KEY (%s, %s), " +  // primary key is table name + column name
                        " INDEX %s (%s)," +  // index column names
                        " INDEX %s (%s) )" +  // index key names
                        " ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName()),
                quote(getColumnColumnName()),
                quote(getKeyColumnName()),
                quote(getTableNameColumnName()), quote(getColumnColumnName()),
                quote(getKeyColumnName()), quote(getKeyColumnName()),
                quote(getColumnColumnName()), quote(getColumnColumnName())
        );
    }

    @Override
    protected String getCreateMapTableSql(final String chunkTableName,
                                          final String namespace,
                                          final Map<String, String> map) {

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE IF NOT EXISTS ")
                .append(getTableFullName(namespace, chunkTableName)).append(" (");
        for (final String key : getKeysOrdered(map)) {
            sqlBuilder.append(quote(getMapKeyColumnName(key))).append(" TEXT, ");
        }
        final int maxIndexedColumns = 63;
        int indexedColumnsCount = 0;
        // add index to as many keys as possible
        final List<String> indexSqls = new ArrayList<>();
        for (final String key : getKeysOrdered(map)) {
            if (++indexedColumnsCount > maxIndexedColumns) {
                // only so many columns can be indexed
                break;
            }
            indexSqls.add(String.format(" INDEX %s (%s (256))",
                    quote(getMapKeyColumnName(key)), quote(getMapKeyColumnName(key))));

        }
        sqlBuilder.append(String.join(",", indexSqls));
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

