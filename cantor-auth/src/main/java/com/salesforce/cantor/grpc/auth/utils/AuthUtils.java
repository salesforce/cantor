package com.salesforce.cantor.grpc.auth.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.auth.Role;
import com.salesforce.cantor.grpc.auth.User;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;

public class AuthUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Create the initial roles users will be able to use
     * @param objects reference to store the generated user
     * @throws IOException thrown from the underlying storage implementation
     */
    public static void initializeRoles(final Objects objects) throws IOException {
        objects.create(Role.ROLES_NAMESPACE);
        objects.store(Role.ROLES_NAMESPACE, Role.FULL_ACCESS.getName(), mapper.writeValueAsBytes(Role.FULL_ACCESS));
    }

    /**
     * Create the initial user
     * @param objects reference to store the generated user
     * @param adminPassword used to later access the admin account
     * @throws IOException thrown from the underlying storage implementation
     */
    public static void initializeAdmin(final Objects objects, final String adminPassword) throws IOException {
        objects.create(User.USER_NAMESPACE);
        objects.store(User.USER_NAMESPACE, User.ADMIN.getUsername(), hashSecret(adminPassword));
    }

    /**
     * Generates random secret key
     * @return secret key
     * @throws NoSuchAlgorithmException if algorithm used doesn't exist
     */
    public static String generateSecretKey() throws NoSuchAlgorithmException {
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
    public static byte[] hashSecret(final String secretKey) throws IOException {
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
    public static byte[] hashSecretWithSalt(final String secretKey, final byte[] salt) throws IOException {
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

    /**
     * Custom exception for errors during role management
     */
    public static class InvalidRoleException extends RuntimeException {
        public InvalidRoleException(final String message) {
            super(message);
        }
    }
}
