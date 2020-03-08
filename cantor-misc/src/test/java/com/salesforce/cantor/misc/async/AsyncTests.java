/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class AsyncTests {
    private static final String path = "/tmp/cantor-async-test/" + UUID.randomUUID().toString();
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    public static Cantor getCantor() throws IOException {
        return new AsyncCantor(new CantorOnH2(path), executor);
    }
}
