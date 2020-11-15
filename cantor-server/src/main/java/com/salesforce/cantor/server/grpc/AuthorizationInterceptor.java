package com.salesforce.cantor.server.grpc;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.auth.AuthorizationConstants;
import io.grpc.*;
import io.jsonwebtoken.*;

import java.io.IOException;

public class AuthorizationInterceptor implements ServerInterceptor {
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
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
        }

        try {
            final byte[] jwtBytes = this.cantor.objects().get(AuthorizationConstants.AUTHORIZATION_NAMESPACE, accessKey);
            if (jwtBytes == null) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Invalid key or secret provided."), metadata);
            }

            final Jwt<Header, String> authorizationJwt = Jwts.parser().parsePlaintextJwt(new String(jwtBytes));
            final Context ctx = Context.current().withValue(Context.key("authorizationJwt"), authorizationJwt.getBody());
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
