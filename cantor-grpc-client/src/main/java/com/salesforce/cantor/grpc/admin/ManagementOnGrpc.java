package com.salesforce.cantor.grpc.admin;

import com.salesforce.cantor.common.credentials.BasicCantorCredentials;
import com.salesforce.cantor.grpc.auth.*;
import com.salesforce.cantor.grpc.auth.AuthorizationServiceGrpc.AuthorizationServiceBlockingStub;
import com.salesforce.cantor.grpc.AbstractBaseGrpcClient;
import com.salesforce.cantor.management.*;
import io.grpc.ManagedChannel;

import java.util.List;

public class ManagementOnGrpc extends AbstractBaseGrpcClient<AuthorizationServiceBlockingStub> implements Users, Roles {
    public ManagementOnGrpc(final ManagedChannel channel) {
        super(AuthorizationServiceGrpc::newBlockingStub, channel);
    }

    @Override
    public void createRole(final String name, final List<String> readAccess, final List<String> writeAccess) {
        getStub().createRole(CreateRoleRequest.newBuilder()
                .setNewRoleName(name)
                .addAllReadAccessNamespaces(readAccess)
                .addAllWriteAccessNamespaces(writeAccess)
                .build());
    }

    @Override
    public CantorCredentials createUser(final String name, List<String> roles) {
        final AccessKeysResponse response = getStub().createUser(CreateUserRequest.newBuilder()
                .setNewUsername(name)
                .addAllRoleNames(roles)
                .build());
        return new BasicCantorCredentials(response.getAccessKey(), response.getSecretKey());
    }
}
