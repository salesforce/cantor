/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server;

import com.google.common.base.Strings;
import com.typesafe.config.Config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class CantorEnvironment {
    private final Map<String, String> environmentVariables = System.getenv();

    private final Config config;

    public CantorEnvironment(final Config config) {
        this.config = config;
    }

    public String getEnvironmentVariable(final String key) {
        return this.environmentVariables.get(key);
    }

    public String getStorageType() {
        return this.config.getString(Constants.CONFIG_ROOT_PREFIX + "." + Constants.CANTOR_STORAGE_TYPE);
    }

    public List<? extends Config> getConfigAsList(final String config) {
        checkArgument(!Strings.isNullOrEmpty(config), "null/empty config path");
        return getIfHasPath(this.config::getConfigList, getConfigPath(config), Collections.emptyList());
    }

    public int getConfigAsInteger(final String config, final int defaultValue) {
        checkArgument(!Strings.isNullOrEmpty(config), "null/empty config path");
        return getIfHasPath(this.config::getInt, getConfigPath(config), defaultValue);
    }

    private <T> T getIfHasPath(final Function<String, T> getter, final String path, final T ifNoPath) {
        if (this.config.hasPath(path)) {
            return getter.apply(path);
        }

        return ifNoPath;
    }

    private String getConfigPath(final String config) {
        return Constants.CONFIG_ROOT_PREFIX + "." + config;
    }
}
