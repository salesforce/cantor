/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseMapsTest;

import java.io.IOException;

public class AsyncMapsTest extends AbstractBaseMapsTest {
    @Override
    public Cantor getCantor() throws IOException {
        return AsyncTests.getCantor();
    }
}