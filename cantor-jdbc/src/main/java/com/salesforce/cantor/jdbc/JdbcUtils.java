/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcUtils {
    public static String quote(final String s) {
        return String.format("`%s`", s);
    }

    static void addParameters(final PreparedStatement preparedStatement, final Object... parameters)
            throws SQLException {
        if (parameters == null) {
            return;
        }
        int index = 0;
        for (final Object param : parameters) {
            index++;
            if (param instanceof Integer) {
                preparedStatement.setInt(index, (Integer) param);
            } else if (param instanceof Long) {
                preparedStatement.setLong(index, (Long) param);
            } else if (param instanceof Boolean) {
                preparedStatement.setBoolean(index, (Boolean) param);
            } else if (param instanceof String) {
                preparedStatement.setString(index, (String) param);
            } else if (param instanceof Double) {
                preparedStatement.setDouble(index, (Double) param);
            } else if (param instanceof Float) {
                preparedStatement.setFloat(index, (Float) param);
            } else if (param instanceof byte[]) {
                preparedStatement.setBytes(index, (byte[]) param);
            } else {
                throw new IllegalStateException("invalid parameter type: " + param);
            }
        }
    }

    static String getPlaceholders(final int count) {
        if (count == 1) {
            return "?";
        }
        final StringBuilder placeholders = new StringBuilder("?");
        for (int i = 1; i < count; i++) {
            placeholders.append(",?");
        }
        return placeholders.toString();
    }
}
