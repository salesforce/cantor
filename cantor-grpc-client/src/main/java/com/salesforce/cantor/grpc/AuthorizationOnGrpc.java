package com.salesforce.cantor.grpc;

import com.salesforce.cantor.common.credentials.BasicCantorCredentials;
import com.salesforce.cantor.grpc.auth.*;
import com.salesforce.cantor.grpc.auth.AuthorizationServiceGrpc.AuthorizationServiceBlockingStub;

public class AuthorizationOnGrpc extends AbstractBaseGrpcClient<AuthorizationServiceBlockingStub> {
    public AuthorizationOnGrpc(final String target) {
        super(AuthorizationServiceGrpc::newBlockingStub, target);
    }

    public BasicCantorCredentials requestAccess(final String username, final String password) {
        final GenerateAccessKeyResponse response = getStub().requestAccess(GenerateAccessKeyRequest.newBuilder()
                .setUsername(username)
                .setPassword(password)
                .build());
        return new BasicCantorCredentials(response.getAccessKey(), response.getSecretKey());
    }
}
