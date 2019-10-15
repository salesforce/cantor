/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import java.util.Collections;
import java.util.Map;

public class CommonUtils {
    public static <K, V> Map<K, V> nullToEmpty(final Map<K, V> map) {
        return map != null ? map : Collections.emptyMap();
    }
}
