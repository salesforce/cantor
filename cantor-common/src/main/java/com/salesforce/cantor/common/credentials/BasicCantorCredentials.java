package com.salesforce.cantor.common.credentials;

public class BasicCantorCredentials implements CantorCredentials {
    private final String accessKey;
    private final String secretKey;

    public BasicCantorCredentials(final String accessKey, final String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }

    @Override
    public String getSecretKey() {
        return this.secretKey;
    }
}
