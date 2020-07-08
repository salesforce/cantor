package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {
    public static void generateData(final Cantor localCantor,
                                    final long timeframeOrigin,
                                    final long timeframeBound,
                                    final Map<String, Long> cantorH2Namespaces) throws IOException {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int namespaceCount = 0; namespaceCount < random.nextInt(2, 5); namespaceCount++) {
            final String namespace = "cantor-archive-test-" + Math.abs(UUID.randomUUID().hashCode());
            localCantor.events().create(namespace);
            cantorH2Namespaces.put(namespace, random.nextLong(timeframeOrigin, timeframeBound));

            for (int eventCount = 0; eventCount < random.nextInt(100, 1000); eventCount++) { // 1GB max
                final byte[] randomPayload = new byte[random.nextInt(0, 1_000_000)]; // 1MB max
                random.nextBytes(randomPayload);
                localCantor.events().store(
                        namespace, random.nextLong(timeframeOrigin, timeframeBound),
                        null,null, randomPayload
                );
            }
            final Map<String, String> metadataMap = new HashMap<>();
            metadataMap.put("test-event-metadata", "test-generate");
            localCantor.events().store(
                    namespace, timeframeOrigin - 1,
                    metadataMap,null, null
            );
            localCantor.events().store(
                    namespace, timeframeBound + 1,
                    metadataMap,null, null
            );
            for (int eventCount = 0; eventCount < random.nextInt(1, 10); eventCount++) { // 1MB max
                // throw in a few random events at zero timestamp
                final byte[] randomPayload = new byte[random.nextInt(0, 100_000)]; // 100KB max
                random.nextBytes(randomPayload);
                localCantor.events().store(
                        namespace, 0,
                        null,null, null
                );
            }
        }
    }

    public static long getFloorForWindow(final long timestampMillis, final long chunkMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }
}
