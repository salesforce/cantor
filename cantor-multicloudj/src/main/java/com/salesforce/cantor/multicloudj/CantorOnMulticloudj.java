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

/**
 * Cantor backed by a multicloudj {@link BucketClient}, allowing storage on any
 * blob substrate that multicloudj supports (AWS S3, GCP Cloud Storage, Alibaba OSS, etc.).
 *
 * <p>This implementation operates against a single bucket; the bucket is bound to the
 * {@link BucketClient} at construction time.
 *
 * <p><b>Authentication / cross-account access:</b> credentials are configured on the
 * {@link BucketClient} before it is passed in. For cross-account or assume-role flows,
 * configure the client via
 * {@code BucketClient.builder(providerId).withCredentialsOverrider(overrider)} using
 * a {@code CredentialsOverrider} from multicloudj's STS module. Cantor itself does not
 * manage credentials.
 */
public class CantorOnMulticloudj implements Cantor {
    private final Objects objects;
    private final Events events;

    public CantorOnMulticloudj(final BucketClient bucketClient) throws IOException {
        this.objects = new ObjectsOnMulticloudj(bucketClient);
        this.events = new EventsOnMulticloudj(bucketClient);
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
}
