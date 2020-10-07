package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringInputStream;
import com.google.common.cache.*;
import com.salesforce.cantor.Namespaceable;
import com.salesforce.cantor.common.CommonPreconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.*;

/**
 * A class responsible for managing namespace level calls for CantorOnS3
 *
 * Note: there is a cache for the namespaces that refreshes every 30 seconds, however this means that there is a chance
 * an instance of Cantor may think that a namespace exists when it doesn't.
 */
public abstract class AbstractBaseS3Namespaceable implements Namespaceable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBaseS3Namespaceable.class);

    protected final AmazonS3 s3Client;
    protected final String bucketName;

    // cantor-all-namespaces-<type>
    private final String namespaceLookupKey;
    private final LoadingCache<String, Optional<String>> namespaceCache;

    public AbstractBaseS3Namespaceable(final AmazonS3 s3Client, final String bucketName, final String type) throws IOException {
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketName, "null/empty bucket name");
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.namespaceLookupKey = S3Utils.getCleanKeyForNamespace(String.format("all-namespaces-%s", type));
        try {
            if (!this.s3Client.doesBucketExistV2(this.bucketName)) {
                throw new IllegalStateException("bucket does not exist: " + this.bucketName);
            }
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating required buckets for objects on s3:", e);
            throw new IOException("exception creating required buckets for objects on s3:", e);
        }

        this.namespaceCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Optional<String>>() {
                final Map<String, String> cachedNamespaces = new HashMap<>();

                @Override
                public Optional<String> load(final String namespace) throws IOException {
                    if (cachedNamespaces.containsKey(namespace)) {
                        return Optional.of(cachedNamespaces.get(namespace));
                    }

                    refreshNamespaces(cachedNamespaces);
                    return Optional.ofNullable(cachedNamespaces.get(namespace));
                }
            });
        // refresh the namespace cache every 30 seconds
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(this::refreshCache, 0, 30, TimeUnit.SECONDS);
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
            // creation failed invalidate cache
            this.namespaceCache.invalidate(namespace);

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

    protected void checkNamespace(final String namespace) throws IOException {
        CommonPreconditions.checkNamespace(namespace);
        final Optional<String> namespaceKey = this.namespaceCache.getUnchecked(namespace);
        if (!namespaceKey.isPresent()) {
            throw new IOException(String.format("namespace '%s' does not exist", namespace));
        }
    }

    private Collection<String> doGetNamespaces() throws IOException {
        final InputStream namespacesCsv = S3Utils.getObjectStream(this.s3Client, this.bucketName, this.namespaceLookupKey);
        if (namespacesCsv == null) {
            return Collections.emptyList();
        }

        try(final BufferedReader namespaceReader = new BufferedReader(new InputStreamReader(namespacesCsv))) {
            return namespaceReader
                    .lines() // get list of csv objects
                    .skip(1) // skip the header
                    .map(namespaceCsv -> namespaceCsv.split(",")[0]) // get the first element which is the namespace
                    .collect(Collectors.toList());
        }
    }

    private void doCreate(final String namespace) throws IOException {
        logger.info("creating namespace '{}' and adding to '{}.{}'", namespace, this.bucketName, this.namespaceLookupKey);
        final byte[] namespacesCsv = S3Utils.getObjectBytes(this.s3Client, this.bucketName, this.namespaceLookupKey);
        this.namespaceCache.put(namespace, Optional.ofNullable(S3Utils.getCleanKeyForNamespace(namespace)));
        if (namespacesCsv == null || namespacesCsv.length == 0) {
            final InputStream csvForNamespaces = new StringInputStream("namespace,key\n" + namespace + "," + S3Utils.getCleanKeyForNamespace(namespace));
            S3Utils.putObject(this.s3Client, this.bucketName, this.namespaceLookupKey, csvForNamespaces, new ObjectMetadata());
            return;
        }

        String namespaces = new String(namespacesCsv);
        final String newNamespace = "\n" + namespace + "," + S3Utils.getCleanKeyForNamespace(namespace);
        if (namespaces.contains(newNamespace)) {
            // no reason to re-store the same list
            return;
        }

        namespaces += newNamespace;
        final ByteArrayInputStream updatedNamespaceList = new ByteArrayInputStream(namespaces.getBytes(StandardCharsets.UTF_8));
        S3Utils.putObject(this.s3Client, this.bucketName, this.namespaceLookupKey, updatedNamespaceList, new ObjectMetadata());
    }

    private void doDrop(final String namespace) throws IOException {
        logger.info("dropping namespace '{}'", namespace);

        // try to delete any data objects first in-case they are orphaned objects
        final String objectKeyPrefix = getObjectKeyPrefix(namespace);
        logger.debug("deleting all objects with prefix '{}.{}'", this.bucketName, objectKeyPrefix);
        S3Utils.deleteObjects(this.s3Client, this.bucketName, objectKeyPrefix);

        logger.debug("deleting namespace record from namespaces object '{}.{}'", this.bucketName, this.namespaceLookupKey);
        final String remainingNamespacesQuery = String.format("select * from s3object s where NOT s.namespace = '%s'", namespace);
        final InputStream namespacesCsv = S3Utils.S3Select.queryObjectCsv(this.s3Client, this.bucketName, this.namespaceLookupKey, remainingNamespacesQuery);

        // read csv file one element at a time
        final ByteArrayOutputStream csvForNamespaces = new ByteArrayOutputStream();
        try (final BufferedReader namespaceEntry = new BufferedReader(new InputStreamReader(namespacesCsv))) {
            csvForNamespaces.write("namespace,key".getBytes(StandardCharsets.UTF_8));
            final Iterator<String> entries = namespaceEntry.lines().iterator();
            while (entries.hasNext()) {
                final String entry = "\n" + entries.next();
                csvForNamespaces.write(entry.getBytes(StandardCharsets.UTF_8));
            }
            final ByteArrayInputStream updatedNamespaces = new ByteArrayInputStream(csvForNamespaces.toByteArray());
            S3Utils.putObject(this.s3Client, this.bucketName, this.namespaceLookupKey, updatedNamespaces, new ObjectMetadata());
        }
        this.namespaceCache.invalidate(namespace);
    }

    private void refreshCache() {
        for (final String key : this.namespaceCache.asMap().keySet()) {
            this.namespaceCache.refresh(key);
        }
    }

    private void refreshNamespaces(final Map<String, String> cachedNamespaces) throws IOException {
        cachedNamespaces.clear();
        final InputStream namespacesCsv = S3Utils.getObjectStream(s3Client, bucketName, namespaceLookupKey);
        if (namespacesCsv == null) {
            return;
        }

        try(final BufferedReader namespaceReader = new BufferedReader(new InputStreamReader(namespacesCsv))) {
            // get list of csv objects and skip the header
            final Iterator<String> namespaceCsv = namespaceReader.lines().skip(1).iterator();
            while(namespaceCsv.hasNext()) {
                final String entry = namespaceCsv.next();
                final String[] entries = entry.split(",");
                if (entries.length != 2) {
                    throw new IOException("Invalid entry in lookup table: " + entry);
                }
                cachedNamespaces.put(entries[0], entries[1]);
            }
        }
    }
}
