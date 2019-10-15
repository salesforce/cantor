/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Objects;
import com.salesforce.cantor.jdbc.AbstractBaseObjectsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class ObjectsOnH2 extends AbstractBaseObjectsOnJdbc implements Objects {

    public ObjectsOnH2(final String path) throws IOException {
        this(H2DataSourceProvider.getDatasource(new H2DataSourceProperties().setPath(path)));
    }

    public ObjectsOnH2(final DataSource dataSource) throws IOException {
        super(dataSource);
    }

    @Override
    protected String getCreateInternalDatabaseSql() {
        return H2Utils.getH2CreateDatabaseSql(getCantorInternalDatabaseName());
    }

    @Override
    protected String getCreateDatabaseSql(final String database) {
        return H2Utils.getH2CreateDatabaseSql(database);
    }

    @Override
    protected String getDropDatabaseSql(final String database) {
        return H2Utils.getH2DropDatabaseSql(database);
    }

    @Override
    protected String getCreateObjectsTableSql(final String namespace) {
        return "CREATE TABLE IF NOT EXISTS " + getTableFullName(namespace, getObjectsTableName()) + "( " +
                " " + quote(getKeyColumnName()) + " VARCHAR NOT NULL, " +
                " " + quote(getValueColumnName()) + " BINARY NOT NULL, " +
                "  PRIMARY KEY (" + quote(getKeyColumnName()) + ") ) "
                ;
    }
}

