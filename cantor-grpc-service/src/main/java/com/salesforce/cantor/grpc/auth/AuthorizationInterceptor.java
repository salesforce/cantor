package com.salesforce.cantor.grpc.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.credentials.User;
import io.grpc.*;
import io.jsonwebtoken.*;

import java.io.IOException;
import java.util.HashMap;

public class AuthorizationInterceptor implements ServerInterceptor {
    public final static Context.Key<User> userContextKey = Context.key("user");
    private final static ObjectMapper mapper = new ObjectMapper();
    private final Cantor cantor;

    public AuthorizationInterceptor(final Cantor cantor) throws IOException {
        this.cantor = cantor;
        cantor.objects().create(AuthorizationConstants.AUTHORIZATION_NAMESPACE);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                                                 final Metadata metadata,
                                                                 final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        final String accessKey = metadata.get(AuthorizationConstants.ACCESS_KEY);
        if (accessKey == null) {
            // throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("No key or secret provided."), metadata);
            // temporarily allowing unauthenticated connections with full access
            final Context ctx = Context.current().withValue(userContextKey, User.ADMIN);
            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
        }

        try {
            final byte[] jwtBytes = this.cantor.objects().get(AuthorizationConstants.AUTHORIZATION_NAMESPACE, accessKey);
            if (jwtBytes == null) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            final Jwt<Header, Claims> authorizationJwt = Jwts.parser().parseClaimsJwt(new String(jwtBytes));
            final String secretKey = metadata.get(AuthorizationConstants.SECRET_KEY);
            if (!authorizationJwt.getBody().get("passwordHash").equals(secretKey)) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }
            final User user = mapper.convertValue(authorizationJwt.getBody().get(AuthorizationConstants.USER_CLAIM, HashMap.class), User.class);
            final Context ctx = Context.current().withValue(userContextKey, user);
            return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
        } catch (final IOException e) {
            final Status status = Status.ABORTED.withDescription("Authentication failed with internal server error").withCause(e);
            serverCall.close(status, metadata);
            return new ServerCall.Listener<ReqT>() {
                // noop
            };
        }
    }

}
