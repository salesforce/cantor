/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Maps;
import com.salesforce.cantor.jdbc.AbstractBaseMapsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class MapsOnH2 extends AbstractBaseMapsOnJdbc implements Maps {

    public MapsOnH2(final String path) throws IOException {
        this(H2DataSourceProvider.getDatasource(new H2DataSourceProperties().setPath(path)));
    }

    public MapsOnH2(final DataSource dataSource) throws IOException {
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
                        " INDEX %s (%s) )",  // index key names
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
    protected String getCreateMapTableSql(final String chunkTableName, final String namespace, final Map<String, String> map) {

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE IF NOT EXISTS ")
                .append(getTableFullName(namespace, chunkTableName)).append(" (");
        for (final String key : getKeysOrdered(map)) {
            sqlBuilder.append(quote(getMapKeyColumnName(key))).append(" VARCHAR, ");
        }
        // add index to as many keys as possible
        final List<String> indexSqls = new ArrayList<>();
        for (final String key : getKeysOrdered(map)) {
            indexSqls.add(String.format(" INDEX (%s) ", quote(getMapKeyColumnName(key))));
        }
        sqlBuilder.append(String.join(",", indexSqls));
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    @Override
    protected String getRegexQuery(final String column) {
        return String.format(" REGEXP_LIKE (%s, ?) ", column);
    }

    @Override
    protected String getNotRegexQuery(final String column) {
        return String.format(" NOT REGEXP_LIKE (%s, ?) ", column);
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
}

