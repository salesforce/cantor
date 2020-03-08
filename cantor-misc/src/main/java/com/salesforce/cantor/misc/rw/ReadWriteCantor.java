/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.*;
import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class ReadWriteCantor implements Cantor {
    private final ReadWriteObjects objects;
    private final ReadWriteSets sets;
    private final ReadWriteMaps maps;
    private final ReadWriteEvents events;

    public ReadWriteCantor(final Cantor writable, final Cantor readable) {
        checkArgument(writable != null, "null writable");
        checkArgument(readable != null, "null readable");

        this.objects = new ReadWriteObjects(writable.objects(), readable.objects());
        this.sets = new ReadWriteSets(writable.sets(), readable.sets());
        this.maps = new ReadWriteMaps(writable.maps(), readable.maps());
        this.events = new ReadWriteEvents(writable.events(), readable.events());
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
    public Maps maps() {
        return this.maps;
    }

    @Override
    public Events events() {
        return this.events;
    }
}
