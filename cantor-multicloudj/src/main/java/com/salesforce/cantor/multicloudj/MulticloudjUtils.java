/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class MulticloudjUtils {
    private static final Logger logger = LoggerFactory.getLogger(MulticloudjUtils.class);
    private static final int DELETE_BATCH_MAX = 100;
    static final String DIRECTORY_FORMATTER_MIN_PATTERN = "yyyy/MM/dd/HH/mm";
    private static final String DIRECTORY_FORMATTER_HOUR_PATTERN = "yyyy/MM/dd/HH/";
    private static final AtomicBoolean rangeDownloadWarningLogged = new AtomicBoolean(false);

    /**
     * Default upper bound on the size of a single blob download (256 MiB).
     * Without this guard, a single malicious or accidentally-large blob could
     * be loaded fully into a {@link ByteArrayOutputStream} and trigger an OOM
     * in the calling JVM (CWE-400). Callers can override via
     * {@link #setMaxDownloadSizeBytes(long)}.
     */
    private static final long DEFAULT_MAX_DOWNLOAD_SIZE_BYTES = 256L * 1024L * 1024L;
    private static volatile long maxDownloadSizeBytes = DEFAULT_MAX_DOWNLOAD_SIZE_BYTES;

    private MulticloudjUtils() {}

    /**
     * Configure the maximum allowed size (in bytes) of a single blob download.
     * Calls to {@link #download(BucketClient, String)} or
     * {@link #downloadRange(BucketClient, String, long, long)} that would
     * exceed this limit throw {@link IOException} instead of OOMing.
     *
     * @param bytes maximum number of bytes to buffer for a single download;
     *              must be strictly positive
     */
    static void setMaxDownloadSizeBytes(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("max download size must be positive");
        }
        maxDownloadSizeBytes = bytes;
    }

    /** Returns the currently configured max-download-size limit, in bytes. */
    static long getMaxDownloadSizeBytes() {
        return maxDownloadSizeBytes;
    }

    static boolean doesObjectExist(BucketClient bc, String key) throws IOException {
        try {
            return bc.doesObjectExist(key, null);
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("doesObjectExist", key, e);
        }
    }

    static void upload(BucketClient bc, String key, byte[] bytes) throws IOException {
        try {
            bc.upload(UploadRequest.builder().withKey(key).build(), bytes);
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("upload", key, e);
        }
    }

    static void upload(BucketClient bc, String key, InputStream in) throws IOException {
        try {
            bc.upload(UploadRequest.builder().withKey(key).build(), in);
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("upload", key, e);
        }
    }

    static void uploadFile(BucketClient bc, String key, File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            bc.upload(UploadRequest.builder().withKey(key).build(), fis);
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("uploadFile", key, e);
        }
    }

    static byte[] download(BucketClient bc, String key) throws IOException {
        try {
            BoundedByteArrayOutputStream bounded = new BoundedByteArrayOutputStream(maxDownloadSizeBytes, key);
            bc.download(DownloadRequest.builder().withKey(key).build(), bounded);
            return bounded.toByteArray();
        } catch (BoundedByteArrayOutputStream.SizeLimitExceededException e) {
            // Cap-exceeded: surface a clear IOException with the configured limit.
            throw new IOException(String.format(
                    "blob '%s' exceeds maximum download size (limit=%d bytes)",
                    key, maxDownloadSizeBytes), e);
        } catch (SubstrateSdkException e) {
            // Some providers may wrap the bounded-stream IOException; unwrap if so.
            if (isBoundedSizeLimitCause(e)) {
                throw new IOException(String.format(
                        "blob '%s' exceeds maximum download size (limit=%d bytes)",
                        key, maxDownloadSizeBytes), e);
            }
            throw wrapAsIOException("download", key, e);
        }
    }

    static byte[] downloadRange(BucketClient bc, String key, long startInclusive, long endInclusive) throws IOException {
        // Refuse upfront if the requested range itself exceeds the cap.
        final long requested = endInclusive - startInclusive + 1;
        if (requested > maxDownloadSizeBytes) {
            throw new IOException(String.format(
                    "requested range for blob '%s' (%d bytes) exceeds maximum download size (limit=%d bytes)",
                    key, requested, maxDownloadSizeBytes));
        }
        try {
            BoundedByteArrayOutputStream bounded = new BoundedByteArrayOutputStream(maxDownloadSizeBytes, key);
            bc.download(DownloadRequest.builder().withKey(key).withRange(startInclusive, endInclusive).build(), bounded);
            return bounded.toByteArray();
        } catch (BoundedByteArrayOutputStream.SizeLimitExceededException e) {
            throw new IOException(String.format(
                    "blob '%s' range exceeds maximum download size (limit=%d bytes)",
                    key, maxDownloadSizeBytes), e);
        } catch (SubstrateSdkException | UnsupportedOperationException e) {
            String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            if (e instanceof UnsupportedOperationException || msg.contains("range")) {
                if (rangeDownloadWarningLogged.compareAndSet(false, true)) {
                    logger.warn("Range download not supported by provider; falling back to full download + slice for key '{}'", key);
                }
                byte[] full = download(bc, key);
                int start = (int) startInclusive;
                int end = (int) Math.min(endInclusive + 1, full.length);
                return Arrays.copyOfRange(full, start, end);
            }
            if (isBoundedSizeLimitCause(e)) {
                throw new IOException(String.format(
                        "blob '%s' range exceeds maximum download size (limit=%d bytes)",
                        key, maxDownloadSizeBytes), e);
            }
            throw wrapAsIOException("downloadRange", key, e);
        }
    }

    private static boolean isBoundedSizeLimitCause(final Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof BoundedByteArrayOutputStream.SizeLimitExceededException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    /**
     * {@link ByteArrayOutputStream} variant that refuses to grow beyond a
     * configured byte limit — the downloader writes blob bytes into this stream,
     * so capping it bounds peak heap usage per download (F1 / CWE-400 guard).
     */
    static final class BoundedByteArrayOutputStream extends ByteArrayOutputStream {
        private final long limit;
        private final String key;

        BoundedByteArrayOutputStream(long limit, String key) {
            this.limit = limit;
            this.key = key;
        }

        @Override
        public synchronized void write(int b) {
            checkSize(1);
            super.write(b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            checkSize(len);
            super.write(b, off, len);
        }

        @Override
        public synchronized void writeBytes(byte[] b) {
            checkSize(b.length);
            super.writeBytes(b);
        }

        private void checkSize(int incoming) {
            long projected = (long) size() + (long) incoming;
            if (projected > limit) {
                throw new SizeLimitExceededException(String.format(
                        "blob '%s' exceeds maximum download size (limit=%d bytes, attempted=%d bytes)",
                        key, limit, projected));
            }
        }

        /** Unchecked so it propagates cleanly through SDK {@code OutputStream.write(...)} callers. */
        static final class SizeLimitExceededException extends RuntimeException {
            private static final long serialVersionUID = 1L;
            SizeLimitExceededException(String msg) {
                super(msg);
            }
        }
    }

    static List<String> listKeys(BucketClient bc, String prefix) throws IOException {
        try {
            List<String> keys = new ArrayList<>();
            Iterator<BlobInfo> it = bc.list(ListBlobsRequest.builder().withPrefix(prefix).build());
            while (it.hasNext()) {
                keys.add(it.next().getKey());
            }
            return keys;
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("listKeys", prefix, e);
        }
    }

    static List<String> listKeysOrdered(BucketClient bc, String prefix, int start, int count) throws IOException {
        try {
            List<String> keys = new ArrayList<>();
            Iterator<BlobInfo> it = bc.list(ListBlobsRequest.builder().withPrefix(prefix).build());
            int index = 0;
            while (it.hasNext()) {
                String key = it.next().getKey();
                if (index >= start) {
                    keys.add(key);
                    if (count >= 0 && keys.size() >= count) {
                        break;
                    }
                }
                index++;
            }
            return keys;
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("listKeysOrdered", prefix, e);
        }
    }

    static int countKeys(BucketClient bc, String prefix) throws IOException {
        try {
            int count = 0;
            Iterator<BlobInfo> it = bc.list(ListBlobsRequest.builder().withPrefix(prefix).build());
            while (it.hasNext()) {
                it.next();
                count++;
            }
            return count;
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("countKeys", prefix, e);
        }
    }

    static void deleteAllUnderPrefix(BucketClient bc, String prefix) throws IOException {
        List<String> keys = listKeys(bc, prefix);
        deleteBatched(bc, keys);
    }

    static void deleteBatched(BucketClient bc, Collection<String> keys) throws IOException {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        try {
            List<String> keyList = new ArrayList<>(keys);
            for (int i = 0; i < keyList.size(); i += DELETE_BATCH_MAX) {
                List<String> batch = keyList.subList(i, Math.min(i + DELETE_BATCH_MAX, keyList.size()));
                Collection<BlobIdentifier> identifiers = new ArrayList<>(batch.size());
                for (String key : batch) {
                    identifiers.add(new BlobIdentifier(key, null));
                }
                bc.delete(identifiers);
            }
        } catch (SubstrateSdkException e) {
            throw wrapAsIOException("deleteBatched", "batch", e);
        }
    }

    static Set<String> getMatchingKeys(BucketClient bc, String namespaceKeyPrefix, long startMs, long endMs) throws IOException {
        if (startMs > endMs) {
            throw new IllegalArgumentException("start must be <= end");
        }

        SimpleDateFormat directoryFormatterMin = new SimpleDateFormat(DIRECTORY_FORMATTER_MIN_PATTERN);
        SimpleDateFormat directoryFormatterHour = new SimpleDateFormat(DIRECTORY_FORMATTER_HOUR_PATTERN);
        directoryFormatterMin.setTimeZone(TimeZone.getTimeZone("UTC"));
        directoryFormatterHour.setTimeZone(TimeZone.getTimeZone("UTC"));

        Set<String> prefixes = new HashSet<>();
        long current = startMs;
        while (current <= endMs) {
            if (current + TimeUnit.HOURS.toMillis(1) <= endMs) {
                prefixes.add(String.format("%s/%s", namespaceKeyPrefix, directoryFormatterHour.format(new Date(current))));
                current = current / (60 * 60 * 1000) * (60 * 60 * 1000);
                current += TimeUnit.HOURS.toMillis(1);
            } else {
                prefixes.add(String.format("%s/%s", namespaceKeyPrefix, directoryFormatterMin.format(new Date(current))));
                current += TimeUnit.MINUTES.toMillis(1);
            }
        }
        prefixes.add(String.format("%s/%s", namespaceKeyPrefix, directoryFormatterMin.format(new Date(endMs))));

        return prefixes;
    }

    static boolean matchesMetadata(Map<String, String> metadata, Map<String, String> query) {
        if (query == null || query.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> entry : query.entrySet()) {
            String key = entry.getKey();
            String queryValue = entry.getValue();
            String actualValue = metadata != null ? metadata.get(key) : null;

            if (actualValue == null) {
                return false;
            }

            if (queryValue.startsWith("!~")) {
                String pattern = queryValue.substring(2);
                if (matchesLikePattern(actualValue, pattern)) {
                    return false;
                }
            } else if (queryValue.startsWith("~")) {
                String pattern = queryValue.substring(1);
                if (!matchesLikePattern(actualValue, pattern)) {
                    return false;
                }
            } else if (queryValue.startsWith("!=")) {
                String expected = queryValue.substring(2);
                if (actualValue.equals(expected)) {
                    return false;
                }
            } else if (queryValue.startsWith("=")) {
                String expected = queryValue.substring(1);
                if (!actualValue.equals(expected)) {
                    return false;
                }
            } else {
                if (!actualValue.equals(queryValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean matchesDimensions(Map<String, Double> dimensions, Map<String, String> query) {
        if (query == null || query.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> entry : query.entrySet()) {
            String key = entry.getKey();
            String queryValue = entry.getValue();
            Double actualValue = dimensions != null ? dimensions.get(key) : null;

            if (actualValue == null) {
                return false;
            }

            if (queryValue.contains("..")) {
                int dotIdx = queryValue.indexOf("..");
                double low = Double.parseDouble(queryValue.substring(0, dotIdx));
                double high = Double.parseDouble(queryValue.substring(dotIdx + 2));
                if (actualValue < low || actualValue > high) {
                    return false;
                }
            } else if (queryValue.startsWith(">=")) {
                double threshold = Double.parseDouble(queryValue.substring(2));
                if (actualValue < threshold) {
                    return false;
                }
            } else if (queryValue.startsWith("<=")) {
                double threshold = Double.parseDouble(queryValue.substring(2));
                if (actualValue > threshold) {
                    return false;
                }
            } else if (queryValue.startsWith(">")) {
                double threshold = Double.parseDouble(queryValue.substring(1));
                if (actualValue <= threshold) {
                    return false;
                }
            } else if (queryValue.startsWith("<")) {
                double threshold = Double.parseDouble(queryValue.substring(1));
                if (actualValue >= threshold) {
                    return false;
                }
            } else if (queryValue.startsWith("!=")) {
                double expected = Double.parseDouble(queryValue.substring(2));
                if (Double.compare(actualValue, expected) == 0) {
                    return false;
                }
            } else if (queryValue.startsWith("=")) {
                double expected = Double.parseDouble(queryValue.substring(1));
                if (Double.compare(actualValue, expected) != 0) {
                    return false;
                }
            } else {
                double expected = Double.parseDouble(queryValue);
                if (Double.compare(actualValue, expected) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    static IOException wrapAsIOException(String op, String namespace, Throwable cause) {
        return new IOException(String.format("exception during %s on namespace '%s'", op, namespace), cause);
    }

    private static boolean matchesLikePattern(String value, String pattern) {
        if (!pattern.contains("*")) {
            return value.equals(pattern);
        }
        String regex = buildRegexFromGlob(pattern);
        return value.matches(regex);
    }

    private static String buildRegexFromGlob(String glob) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (c == '*') {
                sb.append(".*");
            } else if ("\\[]{}()^$.|?+".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
