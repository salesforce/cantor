package com.salesforce.cantor.grpc.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.management.Roles;
import com.salesforce.cantor.management.Users;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class AuthorizationGrpcService extends AuthorizationServiceGrpc.AuthorizationServiceImplBase {
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Cantor cantor;

    public AuthorizationGrpcService(final Cantor cantor, final String adminPassword) throws IOException {
        checkArgument(cantor != null, "null cantor");
        // create user namespace
        cantor.objects().create(UserConstants.USER_NAMESPACE);
        cantor.objects().store(UserConstants.USER_NAMESPACE,
                Users.ADMIN.getUsername(),
                UserUtils.hashSecret(adminPassword));
        // create roles namespace
        cantor.objects().create(UserConstants.ROLES_NAMESPACE);
        cantor.objects().store(UserConstants.ROLES_NAMESPACE,
                Roles.FULL_ACCESS.getName(),
                mapper.writeValueAsBytes(Roles.FULL_ACCESS));
        this.cantor = cantor;
    }

    @Override
    public void createUser(final CreateUserRequest request, final StreamObserver<AccessKeysResponse> responseObserver) {
        if (!UserUtils.isAdmin()) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            // create access and secret key
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final String accessKey = encoder.encodeToString(md.digest(request.getNewUsernameBytes().toByteArray()));
            final String secretKey = UserUtils.generateSecretKey();

            // generate and store new jwt
            final byte[] secretHash = UserUtils.hashSecret(secretKey);
            getObjects().store(UserConstants.USER_NAMESPACE, accessKey, generateUser(request, accessKey, secretHash).getBytes());

            final AccessKeysResponse accessKeys = AccessKeysResponse.newBuilder()
                    .setAccessKey(accessKey)
                    .setSecretKey(secretKey)
                    .build();
            sendResponse(responseObserver, accessKeys);
        } catch (final IOException | NoSuchAlgorithmException e) {
            sendError(responseObserver, e);
        } catch (final UserUtils.InvalidRoleException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withCause(e)));
        }
    }

    @Override
    public void createRole(final CreateRoleRequest request, final StreamObserver<EmptyResponse> responseObserver) {
        if (!UserUtils.isAdmin()) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final String roleKey = request.getNewRoleName().toUpperCase();
            final byte[] role = getObjects().get(UserConstants.ROLES_NAMESPACE, roleKey);
            if (role != null) {
                responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription("This role already exists: " + roleKey)));
            }

            // generate and store new jwt
            final Roles.Role newRole = new Roles.Role(roleKey, request.getReadAccessNamespacesList(), request.getWriteAccessNamespacesList());
            getObjects().store(UserConstants.ROLES_NAMESPACE, roleKey, mapper.writeValueAsBytes(newRole));

            sendResponse(responseObserver, EmptyResponse.getDefaultInstance());
        } catch (final IOException e) {
            sendError(responseObserver, e);
        }
    }

    private String generateUser(final CreateUserRequest request,
                                final String userHash,
                                final byte[] secretHash) throws IOException {
        // to upper all roles to make roles case-insensitive
        final List<String> roleNamesList = request
                .getRoleNamesList()
                .stream()
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        // get all roles and validate all requested roles exist
        final Collection<String> roles = getObjects().keys(UserConstants.ROLES_NAMESPACE, 0, -1);
        for (final String role : roleNamesList) {
            final String roleKey = role.toUpperCase();
            if (!roles.contains(roleKey)) {
                throw new UserUtils.InvalidRoleException("The requested role does not exist: " + roleKey);
            }
        }

        final Users.User newUser = new Users.User(request.getNewUsername(), Users.Status.ACTIVE, roleNamesList);
        return Jwts.builder()
                .setSubject(userHash)
                .claim(UserConstants.USER_CLAIM, mapper.writeValueAsString(newUser))
                .claim(UserConstants.PASSWORD_CLAIM, encoder.encodeToString(secretHash))
                .compact();
    }

    private Objects getObjects() {
        return this.cantor.objects();
    }
}
