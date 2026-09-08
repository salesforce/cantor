/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;

/**
 * Test-only {@link BucketClient} subclass that wraps a directly-constructed {@link AbstractBlobStore}.
 *
 * <p>The standard {@code BucketClient.builder(providerId)} path uses Java SPI ({@code ServiceLoader})
 * to discover providers, but as of multicloudj 0.3.5 the in-memory provider does not expose the
 * static factory method that the SPI lookup expects. Subclassing the protected
 * {@link BucketClient#BucketClient(AbstractBlobStore)} constructor lets tests build a working
 * {@code BucketClient} backed by {@code InMemoryBlobStore} without going through SPI.
 */
public final class InMemoryTestBucketClient extends BucketClient {
    public InMemoryTestBucketClient(final AbstractBlobStore blobStore) {
        super(blobStore);
    }
}
