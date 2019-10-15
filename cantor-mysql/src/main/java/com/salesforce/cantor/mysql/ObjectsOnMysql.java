/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.salesforce.cantor.Objects;
import com.salesforce.cantor.jdbc.AbstractBaseObjectsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class ObjectsOnMysql extends AbstractBaseObjectsOnJdbc implements Objects {

    public ObjectsOnMysql(final String hostname, final int port, final String username, final String password)
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

    public ObjectsOnMysql(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateObjectsTableSql(final String namespace) {
        return "CREATE TABLE IF NOT EXISTS " + getTableFullName(namespace, getObjectsTableName()) + "( " +
                quote(getKeyColumnName()) + " VARCHAR(256) NOT NULL, " +
                quote(getValueColumnName()) + " LONGBLOB NOT NULL, " +
                " PRIMARY KEY (" + quote(getKeyColumnName()) + "), " +
                " UNIQUE INDEX (" + quote(getKeyColumnName()) + ") ) " +
                " ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY KEY(" + quote(getKeyColumnName()) + ") PARTITIONS 10";
    }
}

