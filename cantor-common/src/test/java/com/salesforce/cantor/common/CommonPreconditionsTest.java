/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import org.testng.annotations.Test;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static org.testng.Assert.assertThrows;

public class CommonPreconditionsTest {

    @Test
    public void testCheckString() {
        assertThrows(IllegalArgumentException.class, () -> checkString(null));
        assertThrows(IllegalArgumentException.class, () -> checkString(""));
        checkString("valid");
    }

    @Test
    public void testCheckArgument() {
        assertThrows(IllegalArgumentException.class, () -> checkArgument(false, "ignored"));
        checkArgument(true, "ignored");
    }

    @Test
    public void testCheckState() {
        assertThrows(IllegalStateException.class, () -> checkState(false, "ignored"));
        checkState(true, "ignored");
    }

    @Test
    public void testCheckNamespace() {
        assertThrows(IllegalArgumentException.class, () -> checkNamespace(null));
        assertThrows(IllegalArgumentException.class, () -> checkNamespace(""));
        checkNamespace("!@#$%^&*()_+=-`~./,<>? 0123456789012345678901234567890123456789012345678901234567890123");
    }

    @Test
    public void testCheckCreate() {
        assertThrows(IllegalArgumentException.class, () -> checkCreate(null));
        assertThrows(IllegalArgumentException.class, () -> checkCreate(""));
        checkCreate("!@#$%^&*()_+=-`~./,<>? 0123456789012345678901234567890123456789012345678901234567890123");
    }

    @Test
    public void testCheckDrop() {
        assertThrows(IllegalArgumentException.class, () -> checkDrop(null));
        assertThrows(IllegalArgumentException.class, () -> checkDrop(""));
        checkDrop("!@#$%^&*()_+=-`~./,<>? 0123456789012345678901234567890123456789012345678901234567890123");
    }
}