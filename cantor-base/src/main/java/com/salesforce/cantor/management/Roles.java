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
public interface Roles {
    Role FULL_ACCESS = new Role("full-access", Collections.singletonList(".*"), Collections.singletonList(".*"));

    class Role {
        private final String name;
        private final List<String> readAccess;
        private final List<String> writeAccess;

        public Role(final String name, final List<String> readAccess, final List<String> writeAccess) {
            this.name = name;
            this.readAccess = readAccess;
            this.writeAccess = writeAccess;
        }

        /**
         * Provides the unique name for this role
         * @return the name of this role
         */
        public String getName() {
            return name;
        }

        /**
         * Evaluates if this role allows read access to the provided namespace
         */
        public boolean hasReadAccess(final String namespace) {
            for (final String allowedNamespace : this.readAccess) {
                if (namespace.matches(allowedNamespace)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Evaluates if this role allows write access to the provided namespace
         */
        public boolean hasWriteAccess(final String namespace) {
            for (final String allowedNamespace : this.writeAccess) {
                if (namespace.matches(allowedNamespace)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Creates a role is it doesn't already exist
     * @param name unique role name that will be used to reference the role
     * @param readAccess the list of regex patterns that match namespaces which this role will allow read access
     * @param writeAccess the list of regex patterns that match namespaces which this role will allow write access
     * @return {@literal true} if the role was create, {@literal false} otherwise
     * @throws IOException exception thrown from the underlying storage implementation
     */
    boolean createRole(final String name, final List<String> readAccess, final List<String> writeAccess) throws IOException;
}
