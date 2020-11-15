package com.salesforce.cantor.grpc.auth;

import java.util.ArrayList;
import java.util.List;

public class Roles {
    private List<String> writeAccess;
    private List<String> readAccess;

    public Roles() {}

    public Roles(final GenerateAccessKeyRequest request) {
        this.writeAccess = new ArrayList<>();
        this.readAccess = new ArrayList<>();

        this.writeAccess.add(".*");
        this.readAccess.add(".*");
    }

    public List<String> getWriteAccess() {
        return writeAccess;
    }

    public List<String> getReadAccess() {
        return readAccess;
    }

    public void setWriteAccess(final List<String> writeAccess) {
        this.writeAccess = writeAccess;
    }

    public void setReadAccess(final List<String> readAccess) {
        this.readAccess = readAccess;
    }

    public boolean hasReadAccess(final String namespace) {
        for (final String allowedNamespace : this.readAccess) {
            if (namespace.matches(allowedNamespace)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasWriteAccess(final String namespace) {
        for (final String allowedNamespace : this.writeAccess) {
            if (namespace.matches(allowedNamespace)) {
                return true;
            }
        }
        return false;
    }
}
