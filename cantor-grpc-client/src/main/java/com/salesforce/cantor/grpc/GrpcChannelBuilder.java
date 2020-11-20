package com.salesforce.cantor.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.salesforce.cantor.grpc.auth.CredentialsProviderInterceptor;
import com.salesforce.cantor.management.CantorCredentials;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ClientAuth;

import javax.net.ssl.*;
import java.io.File;
import java.util.concurrent.Executors;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

/**
 * Builder class for creating a {@link ManagedChannel}
 */
public class GrpcChannelBuilder {
    private final NettyChannelBuilder channelBuilder;

    /**
     * Builder defaults to TLS security
     */
    public static GrpcChannelBuilder newBuilder(final String target) {
        return new GrpcChannelBuilder(target);
    }

    /**
     * Set credentials for authN/authZ with the Cantor server
     * @param credentials access and secret key used to authenticate with the server (set to {@literal null} for an unauthenticated user)
     */
    public GrpcChannelBuilder setCredentials(final CantorCredentials credentials) {
        this.channelBuilder.intercept(new CredentialsProviderInterceptor(credentials));
        return this;
    }

    /**
     * Creates a grpc channel that is unencrypted
     * @return unencrypted gRPC channel
     */
    public GrpcChannelBuilder usePlainText() {
        this.channelBuilder.usePlaintext(true);
        return this;
    }

    /**
     * Creates a grpc channel that uses mutual TLS
     * @param trustManager client trust manger for connection to the server
     * @param keyManager key manager for authentication with the server
     * @return mTLS gRPC channel
     * @throws SSLException if ssl context generation fails
     */
    public GrpcChannelBuilder useMtls(final TrustManagerFactory trustManager,
                                      final KeyManagerFactory keyManager) throws SSLException {
        this.channelBuilder
                .sslContext(GrpcSslContexts.forClient()
                        .clientAuth(ClientAuth.REQUIRE)
                        .trustManager(trustManager)
                        .keyManager(keyManager)
                        .build())
                .build();
        return this;
    }

    /**
     * Creates a grpc channel that uses mutual TLS
     * @param trustCertFile certificate authority shared with the server
     * @param certificateFile certificate file for authentication
     * @param keyFile key file for authentication
     * @return mTLS gRPC channel
     * @throws SSLException if ssl context generation fails
     */
    public GrpcChannelBuilder useMtls(final File trustCertFile,
                                      final File certificateFile,
                                      final File keyFile) throws SSLException {
        this.channelBuilder
                .sslContext(GrpcSslContexts.forClient()
                        .clientAuth(ClientAuth.REQUIRE)
                        .trustManager(trustCertFile)
                        .keyManager(certificateFile, keyFile)
                        .build())
                .build();
        return this;
    }

    /**
     * Build the grpc channel
     */
    public ManagedChannel build() {
        return this.channelBuilder.build();
    }

    private GrpcChannelBuilder(final String target) {
        checkString(target, "null/empty target");
        this.channelBuilder = NettyChannelBuilder.forTarget(target)
                .maxInboundMessageSize(32 * 1024 * 1024)  // 32MB
                .executor(
                        Executors.newFixedThreadPool(
                                16, // exactly 16 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-client-channel-%d").build())
                );
    }
}
