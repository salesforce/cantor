package com.salesforce.cantor.common.credentials;

/**
 * Cantor authentication and authorization are managed from this interface. The access key is an assertion of who you
 * are, and the secret key validates that claim.
 */
public interface CantorCredentials {
    /**
     *  Cantor will use this to lookup the users information.
     */
    String getAccessKey();

    /**
     * Before actions can be taken Cantor will verify that the user identified provided the proper secret key.
     */
    String getSecretKey();
}
