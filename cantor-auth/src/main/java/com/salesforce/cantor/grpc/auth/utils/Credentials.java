package com.salesforce.cantor.grpc.auth.utils;

/**
 * Cantor authentication and authorization are managed from this interface. The access key is an assertion of who you
 * are, and the secret key validates that claim.
 */
public class Credentials {
    private final String accessKey;
    private final String secretKey;

    public Credentials(final String accessKey, final String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    /**
     *  Cantor will use this to lookup the users information.
     */
    public String getAccessKey() {
        return this.accessKey;
    }

    /**
     * Before actions can be taken Cantor will verify that the user identified provided the proper secret key.
     */
    public String getSecretKey() {
        return this.secretKey;
    }
}
