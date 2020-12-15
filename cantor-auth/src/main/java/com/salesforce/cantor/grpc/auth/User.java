package com.salesforce.cantor.grpc.auth;

import java.util.*;

/**
 * Cantor Authentication and Authorization is based on two main systems: {@link User} and {@link Role}
 * <br><br>
 * <h3>User:</h3>
 * <p>A User can have any number of roles assigned to them. Created to be the backend's access management tool, this is
 * meant to reduce the scope of a user's activities or track usage.</p>
 * <br>
 * <h3>Role:</h3>
 * <p>Role defines the read and write access of individual namespaces. Both lists will accept regex patterns to match
 * against namespaces.</p>
 */
public class User {
    public static final String USER_NAMESPACE = ".users";
    public static final User ADMIN = new User("ADMIN", Collections.singletonList(Role.FULL_ACCESS));
    // TODO: temporarily giving unauthenticated users full access
    public static final User ANONYMOUS = new User("ANONYMOUS", Collections.singletonList(Role.FULL_ACCESS));

    private final String username;
    private final List<Role> roles;

    public User(final String username, final List<Role> initialRoles) {
        this.username = username;
        this.roles = initialRoles;
    }

    /**
     * Provides the unique name for this role
     * @return the name of this role
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Provides a copied list of roles for this user
     */
    public List<Role> getRoles() {
        return (this.roles != null) ? new LinkedList<>(this.roles) : null;
    }
}
