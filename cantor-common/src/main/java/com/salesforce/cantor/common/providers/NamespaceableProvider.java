/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common.providers;

import com.salesforce.cantor.Namespaceable;

interface NamespaceableProvider<T extends Namespaceable> {

    String getName();

    T getInstance();
}
