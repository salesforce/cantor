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
public class Role {
    public static final String ROLES_NAMESPACE = ".roles";
    public static final List<String> WRITE_METHODS = Arrays.asList("store", "delete", "expire", "add", "create", "drop");
    public static final List<String> READ_METHODS = Arrays.asList("get", "aggregate", "metadata", "entries", "union", "intersect", "sets", "size", "weight", "inc", "keys");
    public static final Role FULL_ACCESS = new Role("FULL-ACCESS", Collections.singletonList(".*"), Collections.singletonList(".*"));

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
        return this.name;
    }

    /**
     * Provides a copied list of namespace regexes that this role has read permissions for
     */
    public List<String> getReadAccess() {
        return readAccess;
    }

    /**
     * Provides a copied list of namespace regexes that this role has write permissions for
     */
    public List<String> getWriteAccess() {
        return writeAccess;
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
