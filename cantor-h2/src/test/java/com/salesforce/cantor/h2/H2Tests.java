/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Cantor;

import java.io.IOException;
import java.util.UUID;

class H2Tests {
    private static final String path = "/tmp/cantor-test-db/" + UUID.randomUUID().toString();

    static Cantor getCantor() throws IOException {
        return new CantorOnH2(path);
    }
}
