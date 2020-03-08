/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.mysql;

import com.salesforce.cantor.Cantor;

import javax.sql.DataSource;
import java.io.IOException;

class MysqlTests {
    private static final Cantor cantor;

    static {
        try {
            cantor = new CantorOnMysql(getDataSource());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Cantor getCantor() throws IOException {
        return cantor;
    }

    private static DataSource getDataSource() {
        return MysqlDataSourceProvider.getDatasource(
                new MysqlDataSourceProperties()
                        .setHostname("localhost")
                        .setPort(3306)
        );
    }

}
