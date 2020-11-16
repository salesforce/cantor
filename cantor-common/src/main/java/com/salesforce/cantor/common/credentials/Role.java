package com.salesforce.cantor.common.credentials;

import java.util.Collections;
import java.util.List;

public class Role {
    public static Role ADMIN = new Role("ADMIN", Collections.singletonList(".*"), Collections.singletonList(".*"));
    public static Role DEFAULT = new Role("DEFAULT", null, Collections.singletonList(".*"));

    private String name;
    private List<String> writeAccess;
    private List<String> readAccess;

    private Role() {}

    private Role(final String name, final List<String> writeAccess, final List<String> readAccess) {
        this.name = name;
        this.writeAccess = writeAccess;
        this.readAccess = readAccess;
    }

    public String getName() {
        return name;
    }

    public List<String> getWriteAccess() {
        return writeAccess;
    }

    public List<String> getReadAccess() {
        return readAccess;
    }

    public void setName(final String name) {
        this.name = name;
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

//    public static class RoleBuilder {
//        private String name;
//        private final List<String> writeAccess;
//        private final List<String> readAccess;
//
//        private RoleBuilder() {
//            this.readAccess = new ArrayList<>();
//            this.writeAccess = new ArrayList<>();
//        }
//
//        public RoleBuilder newBuilder() {
//            return this;
//        }
//
//        public RoleBuilder setName(final String name) {
//            this.name = name;
//            return this;
//        }
//
//        public RoleBuilder addReadPermissions(final String... namespaceRegexes) {
//            this.readAccess.addAll(Arrays.asList(namespaceRegexes));
//            return this;
//        }
//
//        public RoleBuilder addWritePermissions(final String... namespaceRegexes) {
//            this.writeAccess.addAll(Arrays.asList(namespaceRegexes));
//            return this;
//        }
//
//        public RoleBuilder addReadPermission(final String namespaceRegex) {
//            this.readAccess.add(namespaceRegex);
//            return this;
//        }
//
//        public RoleBuilder addWritePermission(final String namespaceRegex) {
//            this.writeAccess.add(namespaceRegex);
//            return this;
//        }
//
//        public Role build() {
//            final List<Pattern> wAccess = new ArrayList<>();
//            for (final String access : this.writeAccess) {
//                wAccess.add(Pattern.compile(access));
//            }
//
//            final List<Pattern> rAccess = new ArrayList<>();
//            for (final String access : this.readAccess) {
//                rAccess.add(Pattern.compile(access));
//            }
//            return new Role(this.name, wAccess, rAccess);
//        }
//    }
}
