package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.*;
import com.salesforce.cantor.grpc.auth.utils.AuthUtils;
import com.salesforce.cantor.misc.proxy.CantorProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public abstract class AuthorizedCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizedCantor.class);

    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public AuthorizedCantor(final Cantor delegate,
                            final String adminPassword) throws IOException {
        checkArgument(delegate != null, "null delegate");

        AuthUtils.initializeRoles(delegate.objects());
        AuthUtils.initializeAdmin(delegate.objects(), adminPassword);
        logger.info("new instance of authorized cantor created");

        // add proxy to intercept calls for authorization
        final CantorProxy cantorProxy = new CantorProxy(
            new AuthorizedProxy<>(delegate.objects()),
            new AuthorizedProxy<>(delegate.sets()),
            new AuthorizedProxy<>(delegate.events()));

        this.objects = cantorProxy.objects();
        this.sets = cantorProxy.sets();
        this.events = cantorProxy.events();
    }

    public abstract User getCurrentUser();

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        return this.sets;
    }

    @Override
    public Events events() {
        return this.events;
    }

    class AuthorizedProxy<T extends Namespaceable> implements InvocationHandler {
        private final T delegate;

        public AuthorizedProxy(final T delegate) {
            this.delegate = delegate;
        }

        public boolean hasAccess(final Role role, final String namespace, final Method method) throws IOException {
            if (Role.READ_METHODS.contains(method.getName())) {
                return role.hasReadAccess(namespace);
            } else if (Role.WRITE_METHODS.contains(method.getName())) {
                return role.hasWriteAccess(namespace);
            }
            throw new IOException(String.format("User requested an unknown method '%s' on namespace '%s'", method.getName(), namespace));
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            // there is only one method that takes no arguments, and that is 'namespaces()'
            if (method.getName().equals("namespaces")) {
                return method.invoke(this.delegate, args);
            }
            if (args == null || args.length == 0) {
                throw new IllegalArgumentException("method '" + method.getName() + "' is not recognized by proxy object");
            }

            // by convention, the first argument to all methods is the namespace
            final String namespace = (String) args[0];

            final User user = getCurrentUser();
            final List<Role> roles = user.getRoles();
            if (roles == null || roles.isEmpty()) {
                throw new IOException(String.format("User not authorized to make '%s' request on namespace '%s'", method.getName(), namespace));
            }
            for (final Role role : roles) {
                if (hasAccess(role, namespace, method)) {
                    return method.invoke(this.delegate, args);
                }
            }
            throw new IOException(String.format("User not authorized to make '%s' request on namespace '%s'", method.getName(), namespace));
        }
    }
}
