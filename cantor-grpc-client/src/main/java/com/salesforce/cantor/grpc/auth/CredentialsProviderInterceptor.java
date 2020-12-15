package com.salesforce.cantor.grpc.auth;

import com.google.common.base.Preconditions;
import com.salesforce.cantor.grpc.auth.utils.Credentials;
import io.grpc.*;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class CredentialsProviderInterceptor implements ClientInterceptor {
    public static final Metadata.Key<String> ACCESS_KEY = Metadata.Key.of("ACCESS-KEY", ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> SECRET_KEY = Metadata.Key.of("SECRET-KEY", ASCII_STRING_MARSHALLER);

    private final Credentials credentials;

    public CredentialsProviderInterceptor(final Credentials credentials) {
        Preconditions.checkNotNull(credentials, "credentials cannot be null");
        this.credentials = credentials;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
                                                               final CallOptions callOptions,
                                                               final Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                headers.put(ACCESS_KEY, credentials.getAccessKey());
                headers.put(SECRET_KEY, credentials.getSecretKey());
                super.start(responseListener, headers);
            }
        };
    }
}
