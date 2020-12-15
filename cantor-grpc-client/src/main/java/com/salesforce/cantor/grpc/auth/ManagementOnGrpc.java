package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.grpc.AbstractBaseGrpcClient;
import com.salesforce.cantor.grpc.auth.utils.Credentials;
import com.salesforce.cantor.grpc.management.*;
import com.salesforce.cantor.grpc.management.AuthorizationServiceGrpc.AuthorizationServiceBlockingStub;
import io.grpc.ManagedChannel;

import java.util.List;

public class ManagementOnGrpc extends AbstractBaseGrpcClient<AuthorizationServiceBlockingStub> {
    public ManagementOnGrpc(final String target, final Credentials credentials) {
        super(AuthorizationServiceGrpc::newBlockingStub, target, credentials);
    }

    public ManagementOnGrpc(final ManagedChannel channel) {
        super(AuthorizationServiceGrpc::newBlockingStub, channel);
    }

    public void createRole(final String name, final List<String> readAccess, final List<String> writeAccess) {
        getStub().createRole(CreateRoleRequest.newBuilder()
                .setNewRoleName(name)
                .addAllReadAccessNamespaces(readAccess)
                .addAllWriteAccessNamespaces(writeAccess)
                .build());
    }

    public Credentials createUser(final String name, List<String> roles) {
        final AccessKeysResponse response = getStub().createUser(CreateUserRequest.newBuilder()
                .setNewUsername(name)
                .addAllRoleNames(roles)
                .build());
        return new Credentials(response.getAccessKey(), response.getSecretKey());
    }
}
