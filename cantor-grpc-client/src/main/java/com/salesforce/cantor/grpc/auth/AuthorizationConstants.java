package com.salesforce.cantor.grpc.auth;

import io.grpc.Metadata;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class AuthorizationConstants {
    public static final String AUTHORIZATION_NAMESPACE = "user-authorization";

    public static final Metadata.Key<String> ACCESS_KEY = Metadata.Key.of("access_key", ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> SECRET_KEY = Metadata.Key.of("secret_key", ASCII_STRING_MARSHALLER);

    public static final String SECRET_CLAIM = "passwordHash";
    public static final String USER_CLAIM = "user";
}
