/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.jdbc.AbstractBaseSetsOnJdbc;

import javax.sql.DataSource;
import java.io.IOException;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public class SetsOnH2 extends AbstractBaseSetsOnJdbc implements Sets {

    public SetsOnH2(final String path) throws IOException {
        this(H2DataSourceProvider.getDatasource(new H2DataSourceProperties().setPath(path)));
    }

    public SetsOnH2(final DataSource dataSource) throws IOException {
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
    protected String getCreateSetsTableSql(final String namespace) {
        return  "CREATE TABLE IF NOT EXISTS " + getTableFullName(namespace, getSetsTableName()) + " ( " +
                " " + quote(getSetKeyColumnName()) + " VARCHAR NOT NULL, " +
                " " + quote(getEntryColumnName()) + " VARCHAR NOT NULL, " +
                " " + quote(getWeightColumnName()) + " BIGINT, " +
                "  PRIMARY KEY (" + quote(getSetKeyColumnName()) + ", " + quote(getEntryColumnName()) + "), " +
                "  INDEX (" + quote(getSetKeyColumnName()) + "), " +
                "  INDEX (" + quote(getWeightColumnName()) + ") ) "
                ;
    }
}

