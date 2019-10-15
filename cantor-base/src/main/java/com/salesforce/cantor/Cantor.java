/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor;

/**
 * Facade for accessing underlying abstractions.
 */
public interface Cantor {

    /**
     * Returns an instance of Objects.
     *
     * @return instance of objects
     */
    Objects objects();

    /**
     * Returns an instance of Sets.
     *
     * @return instance of sets
     */
    Sets sets();

    /**
     * Returns an instance of Maps.
     *
     * @return instance of maps
     */
    Maps maps();

    /**
     * Returns an instance of Events.
     *
     * @return instance of events
     */
    Events events();
}
