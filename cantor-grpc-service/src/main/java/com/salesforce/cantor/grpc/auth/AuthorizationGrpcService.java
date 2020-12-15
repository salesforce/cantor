package com.salesforce.cantor.grpc.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.common.CommonPreconditions;
import com.salesforce.cantor.grpc.GrpcUtils;
import com.salesforce.cantor.grpc.auth.utils.AuthUtils;
import com.salesforce.cantor.grpc.management.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;


public class AuthorizationGrpcService extends AuthorizationServiceGrpc.AuthorizationServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizationGrpcService.class);
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Cantor cantor;

    public AuthorizationGrpcService(final Cantor cantor) throws IOException {
        CommonPreconditions.checkArgument(cantor != null, "null cantor");
        this.cantor = cantor;
    }

    @Override
    public void createUser(final CreateUserRequest request, final StreamObserver<AccessKeysResponse> responseObserver) {
        if (!isAdmin()) {
            logger.warn("unauthorized request from non-admin: request={}", request);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(String.format("User not authorized to make this request: request=%s", request))));
            return;
        }
        if (Context.current().isCancelled()) {
            GrpcUtils.sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            // create access and secret key
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final String accessKey = encoder.encodeToString(md.digest(UUID.randomUUID().toString().getBytes()));
            final String secretKey = AuthUtils.generateSecretKey();

            // generate and store new jwt
            final byte[] secretHash = AuthUtils.hashSecret(secretKey);
            getObjects().store(User.USER_NAMESPACE, accessKey, generateUser(request, accessKey, secretHash).getBytes());

            final AccessKeysResponse accessKeys = AccessKeysResponse.newBuilder()
                    .setAccessKey(accessKey)
                    .setSecretKey(secretKey)
                    .build();
            GrpcUtils.sendResponse(responseObserver, accessKeys);
        } catch (final IOException | NoSuchAlgorithmException e) {
            GrpcUtils.sendError(responseObserver, e);
        } catch (final AuthUtils.InvalidRoleException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withCause(e)));
        }
    }

    @Override
    public void createRole(final CreateRoleRequest request, final StreamObserver<EmptyResponse> responseObserver) {
        if (!isAdmin()) {
            logger.warn("unauthorized request from non-admin: request={}", request);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(String.format("User not authorized to make this request: request=%s", request))));
            return;
        }
        if (Context.current().isCancelled()) {
            GrpcUtils.sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final String roleKey = request.getNewRoleName().toUpperCase();
            final byte[] role = getObjects().get(Role.ROLES_NAMESPACE, roleKey);
            if (role != null) {
                responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription("This role already exists: " + roleKey)));
            }

            // generate and store new jwt
            final Role newRole = new Role(roleKey, request.getReadAccessNamespacesList(), request.getWriteAccessNamespacesList());
            getObjects().store(Role.ROLES_NAMESPACE, roleKey, mapper.writeValueAsBytes(newRole));

            GrpcUtils.sendResponse(responseObserver, EmptyResponse.getDefaultInstance());
        } catch (final IOException e) {
            GrpcUtils.sendError(responseObserver, e);
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
        final List<Role> validRoles = new ArrayList<>();
        for (final String role : roleNamesList) {
            final String roleKey = role.toUpperCase();
            final byte[] roleBytes = getObjects().get(Role.ROLES_NAMESPACE, roleKey);
            if (roleBytes == null || roleBytes.length == 0) {
                throw new AuthUtils.InvalidRoleException("The requested role does not exist: " + roleKey);
            }
            validRoles.add(UserUtil.jsonToRole(mapper.readTree(roleBytes)));
        }

        final User newUser = new User(request.getNewUsername(), validRoles);
        return Jwts.builder()
                .setSubject(userHash)
                .claim(UserUtil.USER_CLAIM, mapper.writeValueAsString(newUser))
                .claim(UserUtil.PASSWORD_CLAIM, encoder.encodeToString(secretHash))
                .compact();
    }

    /**
     * Simple check of the grpc session for if this is admin user
     */
    private static boolean isAdmin() {
        final User user = UserUtil.CONTEXT_KEY_USER.get(Context.current());
        return user != null && user.getUsername().equals(User.ADMIN.getUsername());
    }

    private Objects getObjects() {
        return this.cantor.objects();
    }
}
