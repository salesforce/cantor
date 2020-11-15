/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.*;
import com.salesforce.cantor.common.credentials.BasicCantorCredentials;
import com.salesforce.cantor.common.credentials.CantorCredentials;

public class CantorOnGrpc implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Events events;
    private final AuthorizationOnGrpc authorization;

    public CantorOnGrpc(final String target) {
        this.objects = new ObjectsOnGrpc(target);
        this.sets = new SetsOnGrpc(target);
        this.events = new EventsOnGrpc(target);
        this.authorization = new AuthorizationOnGrpc(target);
    }

    public CantorOnGrpc(final String target, final CantorCredentials credentials) {
        this.objects = new ObjectsOnGrpc(target, credentials);
        this.sets = new SetsOnGrpc(target, credentials);
        this.events = new EventsOnGrpc(target, credentials);
        this.authorization = new AuthorizationOnGrpc(target);
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

    public BasicCantorCredentials requestAccess(final String username, final String password) {
        return this.authorization.requestAccess(username, password);
    }
}
