/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.Namespaceable;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;

/**
 * A class responsible for managing namespace-level calls for CantorOnMulticloudj.
 *
 * <p>Namespace existence is recorded via an empty marker object at
 * {@code <objectKeyPrefix>/.namespace}. Drop deletes everything under the prefix.
 */
public abstract class AbstractBaseMulticloudjNamespaceable implements Namespaceable {
    protected static final String NAMESPACE_IDENTIFIER = ".namespace";
    private static final Logger logger = LoggerFactory.getLogger(AbstractBaseMulticloudjNamespaceable.class);

    protected final BucketClient bucketClient;

    public AbstractBaseMulticloudjNamespaceable(final BucketClient bucketClient) throws IOException {
        checkArgument(bucketClient != null, "null bucket client");
        this.bucketClient = bucketClient;
        try {
            if (!this.bucketClient.doesBucketExist()) {
                throw new IOException("bucket does not exist: " + bucketClient.getBucket());
            }
        } catch (final SubstrateSdkException e) {
            logger.warn("exception validating bucket client and bucket:", e);
            throw new IOException("exception validating bucket client and bucket", e);
        }
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        try {
            doCreate(namespace);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception creating namespace: " + namespace, e);
            throw new IOException("exception creating namespace: " + namespace, e);
        }
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        try {
            doDrop(namespace);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception dropping namespace: " + namespace, e);
            throw new IOException("exception dropping namespace: " + namespace, e);
        }
    }

    /**
     * Given a namespace this should return the prefix to data object keys.
     */
    protected abstract String getObjectKeyPrefix(final String namespace);

    private void doCreate(final String namespace) {
        logger.info("creating namespace: '{}'.'{}'", this.bucketClient.getBucket(), namespace);
        final String markerKey = getObjectKeyPrefix(namespace) + "/" + NAMESPACE_IDENTIFIER;
        if (MulticloudjUtils.doesObjectExist(this.bucketClient, markerKey)) {
            logger.info("namespace already exists: '{}'.'{}'", namespace, this.bucketClient.getBucket());
            return;
        }
        final byte[] markerBytes = ("namespace=" + namespace).getBytes(StandardCharsets.UTF_8);
        try (final InputStream stream = new ByteArrayInputStream(markerBytes)) {
            MulticloudjUtils.putObject(this.bucketClient, markerKey, stream, markerBytes.length);
        } catch (final IOException e) {
            // ByteArrayInputStream.close() is a no-op; this catch is just to satisfy the compiler.
            throw new SubstrateSdkException("unexpected error closing in-memory stream", e);
        }
    }

    private void doDrop(final String namespace) {
        logger.info("dropping namespace: '{}'.'{}'", this.bucketClient.getBucket(), namespace);
        final String objectKeyPrefix = getObjectKeyPrefix(namespace);
        logger.debug("deleting all objects with prefix '{}.{}'", this.bucketClient.getBucket(), objectKeyPrefix);
        MulticloudjUtils.deleteObjects(this.bucketClient, objectKeyPrefix);
    }

    protected static String trim(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase();
        return String.format("%s-%s",
                cleanName.substring(0, Math.min(64, cleanName.length())),
                Math.abs(namespace.hashCode())
        );
    }
}
