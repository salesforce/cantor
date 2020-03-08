/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;

import java.io.IOException;
import java.util.UUID;

class ReadWriteTests {
    private static final String path = "/tmp/cantor-async-test/" + UUID.randomUUID().toString();

    public static Cantor getCantor() throws IOException {
        final Cantor cantor = new CantorOnH2(path);
        return new ReadWriteCantor(cantor, cantor);
    }
}
