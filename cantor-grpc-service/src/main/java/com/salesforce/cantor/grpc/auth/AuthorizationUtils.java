package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.common.credentials.User;
import io.grpc.Context;

public class AuthorizationUtils {

    static boolean writeRequestInvalid(final String namespace) {
        final User user = AuthorizationInterceptor.userContextKey.get(Context.current());
        return user == null || !user.hasWritePermission(namespace);
    }

    static boolean readRequestInvalid(final String namespace) {
        final User user = AuthorizationInterceptor.userContextKey.get(Context.current());
        return user == null || !user.hasReadPermission(namespace);
    }

    static class UnauthorizedException extends Throwable {
        public UnauthorizedException(final String message) {
            super(message);
        }
    }
}
