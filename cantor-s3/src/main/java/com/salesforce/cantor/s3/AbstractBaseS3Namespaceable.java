package com.salesforce.cantor.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringInputStream;
import com.salesforce.cantor.Namespaceable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import static com.salesforce.cantor.common.CommonPreconditions.*;

/**
 * A class responsible for managing namespace level calls for CantorOnS3
 * <p>
 * Note: there is a cache for the namespaces that refreshes every 30 seconds, however this means that there is a chance
 * an instance of Cantor may think that a namespace exists when it doesn't.
 * </p>
 */
public abstract class AbstractBaseS3Namespaceable implements Namespaceable {
    protected static final String NAMESPACE_IDENTIFIER = ".namespace";
    private static final Logger logger = LoggerFactory.getLogger(AbstractBaseS3Namespaceable.class);

    protected final AmazonS3 s3Client;
    protected final String bucketName;

    public AbstractBaseS3Namespaceable(final AmazonS3 s3Client, final String bucketName, final String type) throws IOException {
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketName, "null/empty bucket name");
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        try {
            // validate s3Client can connect; valid connection/credentials if exception isn't thrown
            this.s3Client.headBucket(new HeadBucketRequest(this.bucketName));
        } catch (final SdkClientException e) {
            logger.warn("exception validating s3 client and bucket:", e);
            throw new IOException("exception validating s3 client and bucket", e);
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
     * Given a namespace this should return the prefix to s3 data object keys
     */
    protected abstract String getObjectKeyPrefix(final String namespace);

    private void doCreate(final String namespace) throws IOException {
        logger.info("creating namespace: '{}'.'{}'", this.bucketName, namespace);
        final String markerKey = getObjectKeyPrefix(namespace) + "/" + NAMESPACE_IDENTIFIER;
        if (S3Utils.doesObjectExist(this.s3Client, this.bucketName, markerKey)) {
            logger.info("namespace already exists: '{}'.'{}'", namespace, this.bucketName);
            return;
        }
        final InputStream csvForNamespaces = new StringInputStream("namespace=" + namespace);
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("text/plain");
        S3Utils.putObject(this.s3Client, this.bucketName, markerKey, csvForNamespaces, objectMetadata);
    }

    private void doDrop(final String namespace) throws IOException {
        logger.info("dropping namespace: '{}'.'{}'", this.bucketName, namespace);

        // try to delete any data objects first in-case they are orphaned objects
        final String objectKeyPrefix = getObjectKeyPrefix(namespace);
        logger.debug("deleting all objects with prefix '{}.{}'", this.bucketName, objectKeyPrefix);
        S3Utils.deleteObjects(this.s3Client, this.bucketName, objectKeyPrefix);
    }

    protected static String trim(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase();
        return String.format("%s-%s",
                cleanName.substring(0, Math.min(64, cleanName.length())),
                Math.abs(namespace.hashCode())
        );
    }
}
