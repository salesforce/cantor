/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class H2Helper {

    public static void dump(final String path, final boolean inMemory, final String dumpFile)
            throws SQLException {
        final H2DataSourceProperties dataSourceBuilder = new H2DataSourceProperties(){}
                .setPath(path)
                .setInMemory(inMemory);
        final Connection connection = H2DataSourceProvider.getDatasource(dataSourceBuilder).getConnection();
        final String sql = String.format("SCRIPT TO '%s' COMPRESSION ZIP", dumpFile);
        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
    }

    public static void load(final String path, final boolean inMemory, final String dumpFile)
            throws SQLException {
        final H2DataSourceProperties dataSourceBuilder = new H2DataSourceProperties(){}
                .setPath(path)
                .setInMemory(inMemory);
        final Connection connection = H2DataSourceProvider.getDatasource(dataSourceBuilder).getConnection();
        final String sql = String.format("RUNSCRIPT FROM '%s' COMPRESSION ZIP", dumpFile);
        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
    }

    public static void drop(final String path, final String database)
            throws SQLException {
        final H2DataSourceProperties dataSourceBuilder = new H2DataSourceProperties(){}
                .setPath(path);
        final Connection connection = H2DataSourceProvider.getDatasource(dataSourceBuilder).getConnection();
        final String sql = String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", database);
        final PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
    }
}
