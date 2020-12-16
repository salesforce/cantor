/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import com.salesforce.cantor.Events;

/**
 * Users have to implement this class to provide an instance of Events to be injected into the CantorFactory.
 */
public interface EventsProvider extends NamespaceableProvider<Events> {
}
