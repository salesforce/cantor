package com.salesforce.cantor.management;

import java.io.IOException;
import java.util.*;

/**
 * Cantor Authentication and Authorization is based on two main systems: {@link Users} and {@link Roles}
 * <br><br>
 * <h3>Users:</h3>
 * <p>Users can have any number of roles assigned to them. Created to be the backend's access management tool, this
 * meant to reduce the scope of a user's activities or track usage.</p>
 * <br>
 * <h3>Roles:</h3>
 * <p>Roles define the read and write access of individual namespaces. Both lists will accept regex patterns to match
 * against namespaces.</p>
 */
public interface Users {
    User ADMIN = new User("ADMIN", Status.ACTIVE, Collections.singletonList(Roles.FULL_ACCESS.getName()));
    // TODO: temporarily giving unauthenticated users full access
    User ANONYMOUS = new User("ANONYMOUS", Status.ACTIVE, Collections.singletonList(Roles.FULL_ACCESS.getName()));

    /**
     * Status is for tracking any restriction placed on the user. The status apply to all the user's roles
     */
    enum Status {
        ACTIVE, // no restrictions
        SUSPENDED, // temporarily paused
        LIMITED // some access restricted (can be a limit on the number of calls or specific types of calls)
    }

    class User {
        private final String username;
        private final Status status;
        private final List<String> roles;

        public User(final String username, final Status status, final List<String> initialRoles) {
            this.username = username;
            this.status = status;
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
         * Provides the current status of this user
         * @return the user's {@link Status}
         */
        public Status getStatus() {
            return this.status;
        }

        /**
         * Provides a copied list of roles for this user
         */
        public List<String> getRoles() {
            return new LinkedList<>(this.roles);
        }
    }

    /**
     * Creates a user will the provided roles assigned to them
     * @param name unique name that will be used to reference the user
     * @param roles by name the user will be given deciding which namespaces they'll have access to
     * @return access credentials needed when using cantor to identify this user
     * @throws IOException exception thrown from the underlying storage implementation
     *
     * Note: this is the only time these credentials can be retrieved from the server; they will not be accessible like
     * this again and must be recreated if lost
     */
    CantorCredentials createUser(final String name, final List<String> roles) throws IOException;
}
