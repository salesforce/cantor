package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.management.Roles;
import com.salesforce.cantor.management.Users;
import io.grpc.*;
import io.jsonwebtoken.*;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.grpc.auth.UserConstants.CONTEXT_KEY_ROLES;
import static com.salesforce.cantor.grpc.auth.UserConstants.CONTEXT_KEY_USER;

public class AuthorizationInterceptor implements ServerInterceptor {
    private final Cantor cantor;

    public AuthorizationInterceptor(final Cantor cantor) {
        this.cantor = cantor;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                                                 final Metadata metadata,
                                                                 final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final String accessKey = metadata.get(UserConstants.ACCESS_KEY);
        // unauthenticated users will be given the status of an anonymous user
        if (accessKey == null) {
            try {
                final Context ctx = Context.current()
                        .withValue(CONTEXT_KEY_USER, Users.ANONYMOUS)
                        .withValue(CONTEXT_KEY_ROLES, attachRoles(Users.ANONYMOUS.getRoles()));
                return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
            } catch (final IOException e) {
                final Status status = Status.INTERNAL.withDescription("Authentication failed with internal server error").withCause(e);
                serverCall.close(status, metadata);
                return new ServerCall.Listener<ReqT>() {/*noop*/};
            }
        }

        final String secretKey = metadata.get(UserConstants.SECRET_KEY);
        if (secretKey == null) {
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
        }
        // check for admin
        if (Users.ADMIN.getUsername().equals(accessKey)) {
            try {
                // the first 16 bytes stored in the hash are the salt
                final byte[] adminSecret = this.cantor.objects().get(UserConstants.USER_NAMESPACE, accessKey);
                final byte[] salt = Arrays.copyOf(adminSecret, 16);
                final byte[] userSecret = UserUtils.hashSecretWithSalt(secretKey, salt);
                if (Arrays.equals(userSecret, adminSecret)) {
                    final Context ctx = Context.current().withValue(CONTEXT_KEY_USER, Users.ADMIN);
                    return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
                }
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            } catch (IOException e) {
                final Status status = Status.ABORTED.withDescription("Authentication failed with internal server error").withCause(e);
                serverCall.close(status, metadata);
                return new ServerCall.Listener<ReqT>() {/*noop*/};
            }
        }

        try {
            final byte[] jwtBytes = this.cantor.objects().get(UserConstants.USER_NAMESPACE, accessKey);
            if (jwtBytes == null) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            final Jwt<Header, Claims> authorizationJwt = Jwts.parser().parseClaimsJwt(new String(jwtBytes));

            // the first 16 bytes stored in the hash are the salt
            final byte[] realSecret = Base64.getDecoder().decode(authorizationJwt.getBody().get(UserConstants.PASSWORD_CLAIM, String.class));
            final byte[] salt = Arrays.copyOf(realSecret, 16);
            final byte[] userSecret = UserUtils.hashSecretWithSalt(secretKey, salt);
            if (!Arrays.equals(realSecret, userSecret)) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            // extract user from jwt
            final String jsonUser = authorizationJwt.getBody().get(UserConstants.USER_CLAIM, String.class);
            final Users.User user = UserUtils.jsonToUser(jsonUser);
            final Context ctx = Context.current()
                    .withValue(CONTEXT_KEY_USER, user)
                    .withValue(CONTEXT_KEY_ROLES, attachRoles(user.getRoles()));
            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
        } catch (final IOException e) {
            final Status status = Status.ABORTED.withDescription("Authentication failed with internal server error").withCause(e);
            serverCall.close(status, metadata);
            return new ServerCall.Listener<ReqT>() {/*noop*/};
        }
    }

    private List<Roles.Role> attachRoles(final List<String> roles) throws IOException {
        final List<Roles.Role> newRoles = new ArrayList<>();
        for (final String role : roles) {
            final String jsonRole = new String(this.cantor.objects().get(UserConstants.ROLES_NAMESPACE, role.toUpperCase()));
            newRoles.add(UserUtils.jsonToRole(jsonRole));
        }
        return newRoles;
    }

}
