/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

class H2Utils {
    static String getH2CreateDatabaseSql(final String database) {
        return String.format("CREATE SCHEMA IF NOT EXISTS %s", quote(database));
    }

    static String getH2DropDatabaseSql(final String database) {
        return String.format("DROP SCHEMA IF EXISTS %s CASCADE", quote(database));
    }
}
