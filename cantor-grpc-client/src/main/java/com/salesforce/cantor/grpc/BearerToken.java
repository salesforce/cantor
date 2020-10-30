package com.salesforce.cantor.grpc;

import io.grpc.*;

import java.util.concurrent.Executor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class BearerToken implements CallCredentials {
    private static final Metadata.Key<String> authorizationMetadataKey = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
    private static final String bearerType = "Bearer";

    private final String value;

    BearerToken(String value) {
        this.value = value;
    }

    @Override
    public void applyRequestMetadata(final MethodDescriptor<?, ?> methodDescriptor,
                                     final Attributes attributes,
                                     final Executor executor,
                                     final MetadataApplier metadataApplier) {
        executor.execute(() -> {
            try {
                final Metadata headers = new Metadata();
                headers.put(authorizationMetadataKey, String.format("%s %s", bearerType, value));
                metadataApplier.apply(headers);
            } catch (final Throwable e) {
                metadataApplier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }

    @Override
    public void thisUsesUnstableApi() {
        // noop
    }
}
