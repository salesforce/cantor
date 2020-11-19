package com.salesforce.cantor.grpc.auth;

import com.google.common.base.Preconditions;
import com.salesforce.cantor.management.CantorCredentials;
import io.grpc.*;

public class CredentialsProviderInterceptor implements ClientInterceptor {
    private final CantorCredentials credentials;

    public CredentialsProviderInterceptor(final CantorCredentials credentials) {
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
                headers.put(UserConstants.ACCESS_KEY, credentials.getAccessKey());
                headers.put(UserConstants.SECRET_KEY, credentials.getSecretKey());
                super.start(responseListener, headers);
            }
        };
    }
}
