/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc;

import java.util.Map;

public class CantorProperties {
    private static final Map<String, String> environmentVariables = System.getenv();

    public static String getKingdom() {
        return environmentVariables.getOrDefault("KINGDOM", "dev");
    }

    public static String getEnvironmentVariable(final String key) {
        return environmentVariables.get(key);
    }

    public static String getEnvironmentVariable(final String key, final String defaultValue) {
        return environmentVariables.getOrDefault(key, defaultValue);
    }
}
