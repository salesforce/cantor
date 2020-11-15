package com.salesforce.cantor.grpc.auth;

import com.google.common.hash.Hashing;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;

import java.io.IOException;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.open.GrpcUtils.*;

public class AuthorizationGrpcService extends AuthorizationServiceGrpc.AuthorizationServiceImplBase {
    private final Cantor cantor;

    public AuthorizationGrpcService(final Cantor cantor) throws IOException {
        checkArgument(cantor != null, "null cantor");
        cantor.objects().create(AuthorizationConstants.AUTHORIZATION_NAMESPACE);
        this.cantor = cantor;
    }

    @Override
    public void requestAccess(final GenerateAccessKeyRequest request,
                              final StreamObserver<GenerateAccessKeyResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final String userHash = Hashing.sha256().hashBytes(request.getUsernameBytes().toByteArray()).toString();
            final String passHash = Hashing.sha256().hashBytes(request.getPasswordBytes().toByteArray()).toString();
            final byte[] bytes = getObjects().get(AuthorizationConstants.AUTHORIZATION_NAMESPACE, userHash);
            if (bytes == null) {
                // generate and store new jwt
                getObjects().store(AuthorizationConstants.AUTHORIZATION_NAMESPACE, userHash, generateUser(request, passHash).getBytes());
            }

            final GenerateAccessKeyResponse response = GenerateAccessKeyResponse.newBuilder()
                    .setAccessKey(userHash)
                    .setSecretKey(passHash)
                    .build();
            sendResponse(responseObserver, response);
        } catch (final IOException e) {
            sendError(responseObserver, e);
        }
    }

    private String generateUser(final GenerateAccessKeyRequest request, final String passHash) {
        return Jwts.builder()
                .claim("passwordHash", passHash)
                .claim("roles", new Roles(request))
                .compact();
    }

    private Objects getObjects() {
        return this.cantor.objects();
    }
}
