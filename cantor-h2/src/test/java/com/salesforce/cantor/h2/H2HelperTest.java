/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.h2;

import com.salesforce.cantor.Objects;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class H2HelperTest {
    @Test
    public void testDumpAndLoad() throws Exception {
        final String database = UUID.randomUUID().toString();
        final String namespace = UUID.randomUUID().toString();
        final String databasePath = "/tmp";
        final String dumpfile = Paths.get(databasePath, "/dump.sql").toString();

        // create some random objects
        final Map<String, byte[]> items = new HashMap<>();
        for (int i = 0; i < 100000; ++i) {
            items.put(UUID.randomUUID().toString(), UUID.randomUUID().toString().getBytes());
        }

        // store them
        final Objects objectsBefore = H2Tests.getCantor().objects();
        objectsBefore.create(namespace);
        objectsBefore.store(namespace, items);

        // dump database to a file
        H2Helper.dump(databasePath, false, dumpfile);

        // drop the database
        H2Helper.drop(databasePath, database);

        // load database from file
        H2Helper.load(databasePath, false, dumpfile);

        // verify objects are the same as before
        final Objects objectsAfterLoad = H2Tests.getCantor().objects();
        for (final Map.Entry<String, byte[]> entry : items.entrySet()) {
            assertEquals(objectsAfterLoad.get(namespace, entry.getKey()), entry.getValue());
        }
    }

}