package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.salesforce.cantor.Namespaceable;
import com.salesforce.cantor.common.CommonPreconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.*;

public abstract class AbstractBaseS3Namespaceable implements Namespaceable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBaseS3Namespaceable.class);

    protected final AmazonS3 s3Client;
    protected final String bucketName;

    public AbstractBaseS3Namespaceable(final AmazonS3 s3Client, final String bucketName) throws IOException {
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketName, "null/empty bucket name");
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        try {
            if (!this.s3Client.doesBucketExistV2(this.bucketName)) {
                this.s3Client.createBucket(this.bucketName);
            }
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating required buckets for objects on s3:", e);
            throw new IOException("exception creating required buckets for objects on s3:", e);
        }
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        try {
            return doGetNamespaces();
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting namespaces", e);
            throw new IOException("exception getting namespaces", e);
        }
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        try {
            doCreate(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating namespace: " + namespace, e);
            throw new IOException("exception creating namespace: " + namespace, e);
        }
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        try {
            doDrop(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception dropping namespace: " + namespace, e);
            throw new IOException("exception dropping namespace: " + namespace, e);
        }
    }

    /**
     * Given a namespace this should return the s3 namespace object key
     */
    protected abstract String getNamespaceKey(final String namespace);

    /**
     * Given a namespace this should return the prefix to s3 data object keys
     */
    protected abstract String getObjectKeyPrefix(final String namespace);

    protected boolean namespaceExists(final String namespace) {
        CommonPreconditions.checkNamespace(namespace);
        return this.s3Client.doesObjectExist(this.bucketName, getNamespaceKey(namespace));
    }

    private Collection<String> doGetNamespaces() throws IOException {
        final String namespacePrefix = getNamespaceKey("");
        return S3Utils.getKeys(this.s3Client, this.bucketName, namespacePrefix)
                .stream()
                .map(namespaceFile -> namespaceFile.substring(namespacePrefix.length()))
                .collect(Collectors.toList());
    }

    private void doCreate(final String namespace) throws IOException {
        final String key = getNamespaceKey(namespace);
        if (namespaceExists(namespace)) {
            logger.debug("namespace '{}' already exists; no need to recreate", namespace);
            return;
        }

        logger.info("creating namespace '{}' at '{}.{}'", namespace, this.bucketName, key);
        // if no exception is thrown, the object was put successfully - ignore response value
        // TODO: in the future we can use this file to keep track of useful namespace level information like encryption type
        S3Utils.putObject(this.s3Client, this.bucketName, key, new ByteArrayInputStream(new byte[0]), new ObjectMetadata());
    }

    private void doDrop(final String namespace) {
        logger.info("dropping namespace '{}'", namespace);

        final String objectKeyPrefix = getObjectKeyPrefix(namespace);
        logger.debug("deleting all objects with prefix '{}.{}'", this.bucketName, objectKeyPrefix);
        S3Utils.deleteObjects(this.s3Client, this.bucketName, objectKeyPrefix);

        // TODO: down the line this method should only drop the namespace object and a background thread will delete the data async
        if (!namespaceExists(namespace)) {
            logger.debug("namespace '{}' doesn't exists; no need to delete", namespace);
            return;
        }

        final String namespaceKey = getNamespaceKey(namespace);
        logger.debug("deleting namespace record object '{}.{}'", this.bucketName, namespaceKey);
        this.s3Client.deleteObject(this.bucketName, namespaceKey);
    }
}
