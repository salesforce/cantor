/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.*;

import javax.sql.DataSource;
import java.io.IOException;

public class CantorOnH2 implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public CantorOnH2(final String path) throws IOException {
        this.objects = new ObjectsOnH2(path);
        this.sets = new SetsOnH2(path);
        this.events = new EventsOnH2(path);
    }

    public CantorOnH2(final DataSource dataSource) throws IOException {
        this.objects = new ObjectsOnH2(dataSource);
        this.sets = new SetsOnH2(dataSource);
        this.events = new EventsOnH2(dataSource);
    }

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        return this.sets;
    }

    @Override
    public Events events() {
        return this.events;
    }
}
