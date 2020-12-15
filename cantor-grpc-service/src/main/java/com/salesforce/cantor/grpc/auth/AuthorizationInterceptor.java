package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.auth.utils.AuthUtils;
import io.grpc.*;
import io.jsonwebtoken.*;

import java.io.IOException;
import java.util.*;

public class AuthorizationInterceptor implements ServerInterceptor {
    private final Cantor cantor;

    public AuthorizationInterceptor(final Cantor cantor) {
        this.cantor = cantor;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                                                 final Metadata metadata,
                                                                 final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final String accessKey = metadata.get(CredentialsProviderInterceptor.ACCESS_KEY);
        // unauthenticated users will be given the status of an anonymous user
        if (accessKey == null) {
            final Context ctx = Context.current().withValue(UserUtil.CONTEXT_KEY_USER, User.ANONYMOUS);
            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
        }

        final String secretKey = metadata.get(CredentialsProviderInterceptor.SECRET_KEY);
        if (secretKey == null) {
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
        }
        // check for admin
        if (User.ADMIN.getUsername().equals(accessKey)) {
            try {
                // the first 16 bytes stored in the hash are the salt
                final byte[] adminSecret = this.cantor.objects().get(User.USER_NAMESPACE, accessKey);
                final byte[] salt = Arrays.copyOf(adminSecret, 16);
                final byte[] userSecret = AuthUtils.hashSecretWithSalt(secretKey, salt);
                if (Arrays.equals(userSecret, adminSecret)) {
                    final Context ctx = Context.current().withValue(UserUtil.CONTEXT_KEY_USER, User.ADMIN);
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
            final byte[] jwtBytes = this.cantor.objects().get(User.USER_NAMESPACE, accessKey);
            if (jwtBytes == null) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            final Jwt<Header, Claims> authorizationJwt = Jwts.parser().parseClaimsJwt(new String(jwtBytes));

            // the first 16 bytes stored in the hash are the salt
            final byte[] realSecret = Base64.getDecoder().decode(authorizationJwt.getBody().get(UserUtil.PASSWORD_CLAIM, String.class));
            final byte[] salt = Arrays.copyOf(realSecret, 16);
            final byte[] userSecret = AuthUtils.hashSecretWithSalt(secretKey, salt);
            if (!Arrays.equals(realSecret, userSecret)) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            // extract user from jwt
            final String jsonUser = authorizationJwt.getBody().get(UserUtil.USER_CLAIM, String.class);
            final User user = UserUtil.jsonToUser(jsonUser, this.cantor);
            final Context ctx = Context.current().withValue(UserUtil.CONTEXT_KEY_USER, user);
            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
        } catch (final IOException e) {
            final Status status = Status.ABORTED.withDescription("Authentication failed with internal server error").withCause(e);
            serverCall.close(status, metadata);
            return new ServerCall.Listener<ReqT>() {/*noop*/};
        }
    }
}
