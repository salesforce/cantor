/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import com.salesforce.multicloudj.blob.client.BucketClient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.salesforce.cantor.common.CommonPreconditions.*;

public final class CantorOnMulticloudj implements Cantor, AutoCloseable {
    private final BucketClient bucketClient;
    private final boolean ownsClient;
    private final ObjectsOnMulticloudj objects;
    private final EventsOnMulticloudj events;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CantorOnMulticloudj(final BucketClient bucketClient) throws IOException {
        checkArgument(bucketClient != null, "null bucket client");
        this.bucketClient = bucketClient;
        this.ownsClient = false;
        this.objects = new ObjectsOnMulticloudj(bucketClient);
        this.events = new EventsOnMulticloudj(bucketClient);
    }

    public CantorOnMulticloudj(final String providerId, final String region, final String bucket) throws IOException {
        checkString(providerId, "invalid provider id");
        checkString(region, "invalid region");
        checkString(bucket, "invalid bucket");

        BucketClient client;
        try {
            client = BucketClient.builder(providerId)
                    .withRegion(region)
                    .withBucket(bucket)
                    .build();
        } catch (RuntimeException e) {
            throw new IOException(
                    String.format("failed to construct BucketClient for provider '%s': ensure blob-%s dependency is on the runtime classpath",
                            providerId, providerId), e);
        }

        this.bucketClient = client;
        this.ownsClient = true;
        this.objects = new ObjectsOnMulticloudj(client);
        this.events = new EventsOnMulticloudj(client);
    }

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        throw new UnsupportedOperationException("Sets are not implemented on multicloudj");
    }

    @Override
    public Events events() {
        return this.events;
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        events.close();
        if (ownsClient) {
            bucketClient.close();
        }
    }
}
