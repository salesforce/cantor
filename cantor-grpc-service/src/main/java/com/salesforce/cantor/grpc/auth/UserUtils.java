package com.salesforce.cantor.grpc.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.management.Roles;
import com.salesforce.cantor.management.Users;
import io.grpc.Context;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;

public class UserUtils {
    public static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Evaluates if this user has read access to the provided namespace
     * TODO: consolidate the read and write requests and accept a method signature?
     */
    static boolean readRequestInvalid(final String namespace) {
        final Users.User user = UserConstants.CONTEXT_KEY_USER.get(Context.current());
        final List<Roles.Role> roles = UserConstants.CONTEXT_KEY_ROLES.get(Context.current());
        if (user == null || roles == null || roles.isEmpty()) {
            return true;
        }

        for (final Roles.Role role : roles) {
            // first evaluate the role and then decide whether the status will allow this action
            if (role.hasReadAccess(namespace) && shouldAllow(user, namespace, role)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Evaluates if this user has write access to the provided namespace
     */
    static boolean writeRequestInvalid(final String namespace) {
        final Users.User user = UserConstants.CONTEXT_KEY_USER.get(Context.current());
        final List<Roles.Role> roles = UserConstants.CONTEXT_KEY_ROLES.get(Context.current());
        if (user == null || roles == null || roles.isEmpty()) {
            return true;
        }

        for (final Roles.Role role : roles) {
            // first evaluate the role and then decide whether the status will allow this action
            if (role.hasWriteAccess(namespace) && shouldAllow(user, namespace, role)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Simple check if this is admin user
     */
    static boolean isAdmin() {
        final Users.User user = UserConstants.CONTEXT_KEY_USER.get(Context.current());
        return user != null && user.getUsername().equals("ADMIN");
    }

    /**
     * Ensure the user's status should allow them to preform an action
     */
    static boolean shouldAllow(final Users.User user, final String namespace, final Roles.Role role) {
        switch (user.getStatus()) {
            case ACTIVE:
                return true;
            case SUSPENDED:
                throw new IllegalStateException(String.format("Your activity for namespace '%s' has been temporarily suspended based suspension of role '%s'.", namespace, role.getName()));
            case LIMITED:
                // TODO: implement a limiter class that can fine tune the user's ability to make calls
            default:
                return false;
        }
    }

    /**
     * Generates random secret key
     * @return secret key
     * @throws NoSuchAlgorithmException if algorithm used doesn't exist
     */
    static String generateSecretKey() throws NoSuchAlgorithmException {
        final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        final SecretKey secretKey = keyGen.generateKey();
        return Base64.getEncoder().encodeToString(secretKey.getEncoded());
    }

    /**
     * Hashes the provided secret key using a random salt prefixed to the hash
     * @param secretKey that should be hashed
     * @return hashed string with salt
     * @throws IOException internal error caused during the hash
     */
    static byte[] hashSecret(final String secretKey) throws IOException {
        final SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);

        return hashSecretWithSalt(secretKey, salt);
    }

    /**
     * Hashes the provided secret key using the salt provided and prefixed to the hash
     * @param secretKey that should be hashed
     * @param salt to use for the hashing
     * @return hashed string with salt
     * @throws IOException internal error caused during the hash
     */
    static byte[] hashSecretWithSalt(final String secretKey, final byte[] salt) throws IOException {
        try {
            final KeySpec spec = new PBEKeySpec(secretKey.toCharArray(), salt, 65536, 128);
            final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            final byte[] secretHash = factory.generateSecret(spec).getEncoded();
            final byte[] finalHash = Arrays.copyOf(salt, salt.length + secretHash.length);
            System.arraycopy(secretHash, 0, finalHash, salt.length, secretHash.length);
            return finalHash;
        } catch (final NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IOException(e);
        }
    }

    static Users.User jsonToUser(final String jsonUser) throws IOException {
        final JsonNode json = mapper.readTree(jsonUser);
        final String username = json.get(UserConstants.JSON_FIELD_USERNAME).asText();
        final Users.Status status = Users.Status.valueOf(json.get(UserConstants.JSON_FIELD_STATUS).asText());
        final Iterator<JsonNode> roles = json.get(UserConstants.JSON_FIELD_ROLES).elements();
        final List<String> userRoles = new ArrayList<>();
        while (roles.hasNext()) {
            userRoles.add(roles.next().asText());
        }
        return new Users.User(username, status, userRoles);
    }

    static Roles.Role jsonToRole(final String jsonRole) throws IOException {
        final JsonNode roleJson = mapper.readTree(jsonRole);
        final String roleName = roleJson.get(UserConstants.JSON_FIELD_ROLE_NAME).asText();

        final List<String> readAccess = new ArrayList<>();
        final Iterator<JsonNode> roleReadAccess = roleJson.get(UserConstants.JSON_FIELD_ROLE_READ).elements();
        while (roleReadAccess.hasNext()) {
            readAccess.add(roleReadAccess.next().asText());
        }

        final List<String> writeAccess = new ArrayList<>();
        final Iterator<JsonNode> roleWriteAccess = roleJson.get(UserConstants.JSON_FIELD_ROLE_WRITE).elements();
        while (roleWriteAccess.hasNext()) {
            writeAccess.add(roleWriteAccess.next().asText());
        }
        return new Roles.Role(roleName, readAccess, writeAccess);
    }

    /**
     * Custom exception for errors during role management
     */
    static class InvalidRoleException extends RuntimeException {
        public InvalidRoleException(final String message) {
            super(message);
        }
    }

    /**
     * Custom exception for when a user is unauthorized
     */
    static class UnauthorizedException extends RuntimeException {
        public UnauthorizedException(final String request) {
            super("User not authorized to make this request: " + request);
        }
    }
}
