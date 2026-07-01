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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.salesforce.cantor.common.CommonPreconditions.*;

public abstract class AbstractBaseMulticloudjNamespaceable implements Namespaceable {
    protected static final String NAMESPACE_IDENTIFIER = ".namespace";
    private static final Logger logger = LoggerFactory.getLogger(AbstractBaseMulticloudjNamespaceable.class);

    protected final BucketClient bucketClient;
    protected final String bucketName;

    protected AbstractBaseMulticloudjNamespaceable(final BucketClient bucketClient, final String type) throws IOException {
        checkArgument(bucketClient != null, "null bucket client");
        this.bucketClient = bucketClient;
        this.bucketName = bucketClient.getBucket();
        try {
            if (!this.bucketClient.doesBucketExist()) {
                throw new IOException(String.format("bucket '%s' is not reachable", this.bucketName));
            }
        } catch (final SubstrateSdkException e) {
            throw new IOException(String.format("bucket '%s' is not reachable", this.bucketName), e);
        }
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        final String markerKey = getObjectKeyPrefix(namespace) + "/" + NAMESPACE_IDENTIFIER;
        try {
            if (MulticloudjUtils.doesObjectExist(this.bucketClient, markerKey)) {
                logger.info("namespace already exists: '{}'.'{}'", namespace, this.bucketName);
                return;
            }
            MulticloudjUtils.upload(this.bucketClient, markerKey,
                    ("namespace=" + namespace).getBytes(StandardCharsets.UTF_8));
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("create", namespace, e);
        }
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        try {
            MulticloudjUtils.deleteAllUnderPrefix(this.bucketClient, getObjectKeyPrefix(namespace));
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("drop", namespace, e);
        }
    }

    protected abstract String getObjectKeyPrefix(final String namespace);

    /**
     * Verifies that {@code namespace} has been registered (i.e. its {@code .namespace}
     * marker object is present in the bucket). Hoisted from {@code ObjectsOnMulticloudj}
     * and {@code EventsOnMulticloudj} to keep namespace-existence semantics in one place.
     */
    protected void checkNamespaceExists(final String namespace) throws IOException {
        final String markerKey = getObjectKeyPrefix(namespace) + "/" + NAMESPACE_IDENTIFIER;
        if (!MulticloudjUtils.doesObjectExist(this.bucketClient, markerKey)) {
            throw new IOException(String.format("namespace '%s' does not exist", namespace));
        }
    }

    protected static String trim(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase();
        return String.format("%s-%s",
                cleanName.substring(0, Math.min(64, cleanName.length())),
                Math.abs(namespace.hashCode())
        );
    }
}
