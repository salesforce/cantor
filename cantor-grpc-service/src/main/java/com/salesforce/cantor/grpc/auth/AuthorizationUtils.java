package com.salesforce.cantor.grpc.auth;

import io.grpc.Context;

public class AuthorizationUtils {

    static boolean writeRequestValid(final String namespace) {
        final Roles roles = AuthorizationInterceptor.userRoles.get(Context.current());
        return roles != null && roles.hasWriteAccess(namespace);
    }

    static boolean readRequestValid(final String namespace) {
        final Roles roles = AuthorizationInterceptor.userRoles.get(Context.current());
        return roles != null && roles.hasReadAccess(namespace);
    }

    static class UnauthorizedException extends Throwable {
        public UnauthorizedException(final String message) {
            super(message);
        }
    }
}
