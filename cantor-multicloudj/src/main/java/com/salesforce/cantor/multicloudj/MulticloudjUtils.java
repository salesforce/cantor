/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This class is responsible for all direct communication with the multicloudj BucketClient.
 * Mirrors the surface of S3Utils so the rest of the cantor-multicloudj backend reads the same
 * way as cantor-s3.
 */
public class MulticloudjUtils {
    private static final Logger logger = LoggerFactory.getLogger(MulticloudjUtils.class);

    private static final int streamingChunkSize = 4 * 1024 * 1024;

    public static Collection<String> getKeys(final BucketClient bucketClient,
                                             final String prefix) {
        return getKeys(bucketClient, prefix, 0, -1);
    }

    public static Collection<String> getKeys(final BucketClient bucketClient,
                                             final String prefix,
                                             final int start,
                                             final int count) {
        final long before = System.nanoTime();
        try {
            final Set<String> keys = new HashSet<>();
            final Iterator<BlobInfo> iterator = bucketClient.list(
                    ListBlobsRequest.builder().withPrefix(prefix).build()
            );
            int index = 0;
            while (iterator.hasNext()) {
                final BlobInfo info = iterator.next();
                if (start > index++) {
                    continue;
                }
                keys.add(info.getKey());
                if (keys.size() == count) {
                    logger.debug("retrieved {}/{} keys, returning early", keys.size(), count);
                    return keys;
                }
            }
            return keys;
        } finally {
            logger.info("get keys - bucket: {} - prefix: {} - start: {} - count: {}; time spent: {}ms",
                    bucketClient.getBucket(), prefix, start, count, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static byte[] getObjectBytes(final BucketClient bucketClient,
                                        final String key) throws IOException {
        return getObjectBytes(bucketClient, key, 0, -1);
    }

    public static byte[] getObjectBytes(final BucketClient bucketClient,
                                        final String key,
                                        final long start,
                                        final long end) throws IOException {
        final long before = System.nanoTime();
        try {
            final DownloadRequest.Builder builder = DownloadRequest.builder().withKey(key);
            if (start >= 0 && end > 0) {
                builder.withRange(start, end);
            } else if (start > 0 && end < 0) {
                builder.withRange(start, null);
            }
            final DownloadResponse response = bucketClient.download(builder.build());
            try (final InputStream inputStream = response.getInputStream();
                 final ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                final byte[] data = new byte[streamingChunkSize];
                int read;
                while ((read = inputStream.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, read);
                }
                buffer.flush();
                return buffer.toByteArray();
            }
        } finally {
            logger.info("get object bytes - bucket: {} - key: {} - start: {} - end: {}; time spent: {}ms",
                    bucketClient.getBucket(), key, start, end, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static boolean doesObjectExist(final BucketClient bucketClient, final String key) {
        final long before = System.nanoTime();
        try {
            return bucketClient.doesObjectExist(key, null);
        } finally {
            logger.info("does object exist - bucket: {} - key: {}; time spent: {}ms",
                    bucketClient.getBucket(), key, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static InputStream getObjectStream(final BucketClient bucketClient, final String key) {
        final long before = System.nanoTime();
        try {
            final DownloadResponse response = bucketClient.download(
                    DownloadRequest.builder().withKey(key).build()
            );
            return response.getInputStream();
        } finally {
            logger.info("get object stream - bucket: {} - key: {}; time spent: {}ms",
                    bucketClient.getBucket(), key, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static void putObject(final BucketClient bucketClient,
                                 final String key,
                                 final InputStream content,
                                 final long contentLength) {
        final long before = System.nanoTime();
        try {
            final UploadRequest request = UploadRequest.builder()
                    .withKey(key)
                    .withContentLength(contentLength)
                    .build();
            bucketClient.upload(request, content);
        } finally {
            logger.info("put object - bucket: {} - key: {}; time spent: {}ms",
                    bucketClient.getBucket(), key, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static boolean deleteObject(final BucketClient bucketClient, final String key) {
        final long before = System.nanoTime();
        try {
            if (!bucketClient.doesObjectExist(key, null)) {
                return false;
            }
            bucketClient.delete(key, null);
            return true;
        } finally {
            logger.info("delete object - bucket: {} - key: {}; time spent: {}ms",
                    bucketClient.getBucket(), key, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static void deleteObjects(final BucketClient bucketClient, final Collection<String> keys) {
        final long before = System.nanoTime();
        try {
            if (keys == null || keys.isEmpty()) {
                return;
            }
            final List<BlobIdentifier> identifiers = new ArrayList<>(keys.size());
            for (final String key : keys) {
                identifiers.add(new BlobIdentifier(key, null));
            }
            bucketClient.delete(identifiers);
        } finally {
            logger.info("delete objects - bucket: {} - keys: {}; time spent: {}ms",
                    bucketClient.getBucket(), keys, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static void deleteObjects(final BucketClient bucketClient, final String prefix) {
        final long before = System.nanoTime();
        try {
            final Iterator<BlobInfo> iterator = bucketClient.list(
                    ListBlobsRequest.builder().withPrefix(prefix).build()
            );
            final List<BlobIdentifier> batch = new ArrayList<>();
            while (iterator.hasNext()) {
                batch.add(new BlobIdentifier(iterator.next().getKey(), null));
            }
            if (!batch.isEmpty()) {
                bucketClient.delete(batch);
            }
        } finally {
            logger.info("delete objects - bucket: {} - prefix: {}; time spent: {}ms",
                    bucketClient.getBucket(), prefix, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    public static int getSize(final BucketClient bucketClient, final String prefix) {
        final long before = System.nanoTime();
        try {
            int totalSize = 0;
            final Iterator<BlobInfo> iterator = bucketClient.list(
                    ListBlobsRequest.builder().withPrefix(prefix).build()
            );
            while (iterator.hasNext()) {
                iterator.next();
                totalSize++;
            }
            return totalSize;
        } finally {
            logger.info("get size - bucket: {} - prefix: {}; time spent: {}ms",
                    bucketClient.getBucket(), prefix, ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    /**
     * Recursively uploads every regular file under {@code sourceDirectory} to the bucket, preserving
     * the relative path structure as the object key. Stand-in for AWS {@code TransferManager.uploadDirectory}.
     *
     * <p>If a {@code namespaceTagger} is provided, it is invoked per object key and its return value
     * (when non-null) is set as the {@code namespace} tag on that object's upload.
     *
     * <p><b>Future optimization:</b> {@code AsyncBucketClient.uploadDirectory} delegates to provider-native
     * implementations (e.g. AWS {@code S3TransferManager}) which can outperform this manual walk.
     * It's not used here because (a) cantor-multicloudj is built around the sync {@code BucketClient},
     * and (b) async providers don't exist for Ali or for the in-memory test backend.
     */
    public static void uploadDirectory(final BucketClient bucketClient,
                                       final File sourceDirectory,
                                       final int parallelism,
                                       final NamespaceTagger namespaceTagger) throws IOException, InterruptedException {
        final long before = System.nanoTime();
        final ExecutorService executor = Executors.newFixedThreadPool(
                parallelism,
                namedThreadFactory("cantor-multicloudj-event-uploader-" + bucketClient.getBucket() + "-worker-%d")
        );
        try {
            final Path sourceRoot = sourceDirectory.toPath();
            final List<Future<?>> futures = new ArrayList<>();
            try (final Stream<Path> walk = Files.walk(sourceRoot)) {
                walk.filter(Files::isRegularFile).forEach(filePath -> {
                    final String key = sourceRoot.relativize(filePath).toString().replace(File.separatorChar, '/');
                    futures.add(executor.submit(() -> uploadOne(bucketClient, key, filePath, namespaceTagger)));
                });
            }
            // wait for all uploads; surface the first failure
            for (final Future<?> future : futures) {
                try {
                    future.get();
                } catch (final ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    throw new IOException("upload failed during directory upload", cause);
                }
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
            logger.info("upload directory - bucket: {} - source: {}; time spent: {}ms",
                    bucketClient.getBucket(), sourceDirectory.getAbsolutePath(),
                    ((System.nanoTime() - before) / 1_000_000)
            );
        }
    }

    private static Void uploadOne(final BucketClient bucketClient,
                                  final String key,
                                  final Path filePath,
                                  final NamespaceTagger namespaceTagger) throws IOException {
        final UploadRequest.Builder builder = UploadRequest.builder()
                .withKey(key)
                .withContentLength(Files.size(filePath))
                .withContentType("text/plain");
        if (namespaceTagger != null) {
            final String namespace = namespaceTagger.tagFor(key);
            if (namespace != null) {
                builder.withTags(Collections.singletonMap("namespace", namespace));
            }
        }
        bucketClient.upload(builder.build(), filePath);
        return null;
    }

    /**
     * Strategy for deriving the {@code namespace} tag from an upload key. Returning null skips tagging.
     */
    @FunctionalInterface
    public interface NamespaceTagger {
        String tagFor(String objectKey);
    }

    public static ThreadFactory namedThreadFactory(final String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    public static String getCleanKeyForNamespace(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-/]", "").toLowerCase();
        return String.format("cantor-%s-%s",
                cleanName.substring(0, Math.min(32, cleanName.length())), Math.abs(namespace.hashCode()));
    }
}
