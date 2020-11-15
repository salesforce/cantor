package com.salesforce.cantor.grpc.auth;

public class UnauthorizedException extends Throwable {
    public UnauthorizedException(final String message) {
        super(message);
    }
}
