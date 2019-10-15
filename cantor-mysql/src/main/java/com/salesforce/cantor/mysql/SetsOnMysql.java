/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.jdbc.AbstractBaseSetsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class SetsOnMysql extends AbstractBaseSetsOnJdbc implements Sets {

    public SetsOnMysql(final String hostname, final int port, final String username, final String password)
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

    public SetsOnMysql(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateSetsTableSql(final String namespace) {
        return "CREATE TABLE IF NOT EXISTS " + getTableFullName(namespace, getSetsTableName()) + " ( " +
                quote(getSetKeyColumnName()) + " VARCHAR(512) NOT NULL, " +
                quote(getEntryColumnName()) + " TEXT NOT NULL, " +
                quote(getWeightColumnName()) + " BIGINT, " +
                " PRIMARY KEY (" + quote(getSetKeyColumnName()) + ", " + quote(getEntryColumnName()) + "(512)), " +
                " INDEX (" + quote(getSetKeyColumnName()) + "), " +
                " INDEX (" + quote(getWeightColumnName()) + ")) " +
                " ENGINE=InnoDB DEFAULT CHARSET=utf8 ";
    }
}

