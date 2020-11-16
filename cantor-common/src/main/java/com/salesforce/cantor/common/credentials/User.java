package com.salesforce.cantor.common.credentials;

import java.util.Collections;
import java.util.List;

public class User {
    public static final User ADMIN = new User("ADMIN", Collections.singletonList(Role.ADMIN));

    private String username;
    private List<Role> roles;

    public User() {}

    public User(final String username, final List<Role> roles) {
        this.username = username;
        this.roles = roles;
    }

    public void setUsername(final String username) {
        this.username = username;
    }

    public void setRoles(final List<Role> roles) {
        this.roles = roles;
    }

    public String getUsername() {
        return this.username;
    }

    public List<Role> getRoles() {
        return this.roles;
    }

    public boolean hasWritePermission(final String namespace) {
        for (final Role role : roles) {
            if (role.hasWriteAccess(namespace)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasReadPermission(final String namespace) {
        for (final Role role : roles) {
            if (role.hasReadAccess(namespace)) {
                return true;
            }
        }
        return false;
    }
}
