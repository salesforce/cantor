/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public class H2DataSourceProperties {

    private String path = "cantor-h2.db";
    private boolean inMemory = false;
    private boolean isCompressed = false;
    private String username = "root";
    private String password = "root";

    public String getPath() {
        return this.path;
    }

    public H2DataSourceProperties setPath(final String path) {
        checkString(path, "null/empty path");
        this.path = path;
        return this;
    }

    public boolean isInMemory() {
        return this.inMemory;
    }

    public H2DataSourceProperties setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
        return this;
    }

    public boolean isCompressed() {
        return this.isCompressed;
    }

    public H2DataSourceProperties setCompressed(boolean compressed) {
        this.isCompressed = compressed;
        return this;
    }

    public String getUsername() {
        return this.username;
    }

    public H2DataSourceProperties setUsername(final String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return this.password;
    }

    public H2DataSourceProperties setPassword(final String password) {
        this.password = password;
        return this;
    }

    @Override
    public String toString() {
        return "H2DataSourceProperties(path: " + getPath() + ";embedded: " + isInMemory() + ";compressed: " + isCompressed() + ")";
    }
}

