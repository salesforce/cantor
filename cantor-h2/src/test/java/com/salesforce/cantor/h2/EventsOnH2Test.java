/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseEventsTest;

import java.io.IOException;

public class EventsOnH2Test extends AbstractBaseEventsTest {
    @Override
    public Cantor getCantor() throws IOException {
        return H2Tests.getCantor();
    }
}

