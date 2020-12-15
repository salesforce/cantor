package com.salesforce.cantor.grpc.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.cantor.Cantor;
import io.grpc.Context;

import java.io.IOException;
import java.util.*;

public class UserUtil {
    public static final String USER_CLAIM = "user";
    public static final String PASSWORD_CLAIM = "passwordHash";
    public static final String JSON_FIELD_USERNAME = "username";
    public static final String JSON_FIELD_ROLES = "roles";

    public static final String JSON_FIELD_ROLE_NAME = "name";
    public static final String JSON_FIELD_ROLE_WRITE = "writeAccess";
    public static final String JSON_FIELD_ROLE_READ = "readAccess";

    public static final Context.Key<User> CONTEXT_KEY_USER = Context.key(USER_CLAIM);

    private static final ObjectMapper mapper = new ObjectMapper();

    public static User jsonToUser(final String jsonUser, final Cantor cantor) throws IOException {
        final JsonNode json = mapper.readTree(jsonUser);
        final String username = json.get(UserUtil.JSON_FIELD_USERNAME).asText();
        final Iterator<JsonNode> roles = json.get(UserUtil.JSON_FIELD_ROLES).elements();
        final List<Role> userRoles = new ArrayList<>();
        while (roles.hasNext()) {
            final Role jsonRole = jsonToRole(roles.next());
            userRoles.add(jsonRole);
        }
        return new User(username, userRoles);
    }

    public static Role jsonToRole(final JsonNode roleJson) throws IOException {
        final String roleName = roleJson.get(UserUtil.JSON_FIELD_ROLE_NAME).asText();

        final List<String> readAccess = new ArrayList<>();
        final Iterator<JsonNode> roleReadAccess = roleJson.get(UserUtil.JSON_FIELD_ROLE_READ).elements();
        while (roleReadAccess.hasNext()) {
            readAccess.add(roleReadAccess.next().asText());
        }

        final List<String> writeAccess = new ArrayList<>();
        final Iterator<JsonNode> roleWriteAccess = roleJson.get(UserUtil.JSON_FIELD_ROLE_WRITE).elements();
        while (roleWriteAccess.hasNext()) {
            writeAccess.add(roleWriteAccess.next().asText());
        }
        return new Role(roleName, readAccess, writeAccess);
    }
}
