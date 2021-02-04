/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common;

import com.salesforce.cantor.*;
import com.salesforce.cantor.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class CantorFactory {
    private static final Logger logger = LoggerFactory.getLogger(CantorFactory.class);
    private static Cantor instance;

    /**
     * Get the instance of Cantor services.
     *
     * @throws IOException any exception thrown from injected providers
     * @return an instance of Cantor
     */
    public static Cantor get() throws IOException {
        if (instance == null) {
            init();
        }
        return instance;
    }

    private static synchronized void init() throws IOException {
        if (instance != null) {
            // already initialized
            return;
        }

        logger.info("initializing cantor factory");

        final Map<String, Objects> objects = new HashMap<>();
        final Map<String, Sets> sets = new HashMap<>();
        final Map<String, Events> events = new HashMap<>();

        load(objects, ObjectsProvider.class);
        load(sets, SetsProvider.class);
        load(events, EventsProvider.class);

        logger.info("cantor factory loaded '{}' objects, '{}' sets, and '{}' events providers",
                objects.size(),
                sets.size(),
                events.size()
        );

        // initialize the cantor proxy instance
        instance = new CantorProxy(objects, sets, events);
    }

    private static <T extends Namespaceable, R extends NamespaceableProvider<T>>
    void load(final Map<String, T> map, final Class<R> provider) throws IOException {
        for (final R p : ServiceLoader.load(provider)) {
            final T instance = p.getInstance();
            final String scope = p.getScope();
            logger.info("provider found with name: '{}' instance-class: '{}'", p.getScope(), instance.getClass().getName());

            // do not allow two providers with the same name
            if (map.containsKey(scope)) {
                logger.error("provider for scope '{}' is already loaded", p.getScope());
                throw new IllegalStateException("duplicate service provider name");
            }
            map.put(scope, instance);
        }
    }

    static class CantorProxy implements Cantor {
        private final Objects objects;
        private final Sets sets;
        private final Events events;

        CantorProxy(final Map<String, Objects> objectsDelegates,
                    final Map<String, Sets> setsDelegates,
                    final Map<String, Events> eventsDelegates) {
            this.objects = (Objects) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class[] { Objects.class },
                    new ObjectsProxy(objectsDelegates)
            );
            this.sets = (Sets) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class[] { Sets.class },
                    new SetsProxy(setsDelegates)
            );
            this.events = (Events) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class[] { Events.class },
                    new EventsProxy(eventsDelegates)
            );
        }

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
    }

    static abstract class NamespaceableProxy<T extends Namespaceable> implements InvocationHandler {
        // scoped namespaces look like this: <scope>.<namespace>
        private static final String scopeDelimiter = ".";
        private final Map<String, T> delegates;

        NamespaceableProxy(final Map<String, T> delegates) {
            this.delegates = delegates;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            // there is only one method that takes no arguments, and that is 'namespaces()'
            if (method.getName().equals("namespaces")) {
                return doNamespaces();
            }
            if (args == null || args.length == 0) {
                throw new IllegalArgumentException("method '" + method.getName() + "' is not recognized by proxy object");
            }

            // by convention, the first argument to all methods is the namespace
            final String scopedNamespace = (String) args[0];

            // if there is no scope in the namespace (i.e., no '.' or starts with it), pick the default instance
            if (!scopedNamespace.contains(scopeDelimiter) || scopedNamespace.startsWith(scopeDelimiter)) {
                return method.invoke(this.delegates.get(NamespaceableProvider.DEFAULT_SCOPE), args);
            }
            final String scope = scopedNamespace.substring(0, scopedNamespace.indexOf(scopeDelimiter));
            final String namespace = scopedNamespace.substring(scopedNamespace.indexOf(scopeDelimiter) + 1);

            checkArgument(this.delegates.containsKey(scope), "invalid scope: " + scope);

            // invoke the proxied method and pass all parameters
            if (args.length == 1) {
                return method.invoke(this.delegates.get(scope), namespace);
            }
            final Object[] newArgs = new Object[args.length];
            newArgs[0] = namespace;
            System.arraycopy(args, 1, newArgs, 1, args.length - 1);
            return method.invoke(this.delegates.get(scope), newArgs);
        }

        private Object doNamespaces() throws IOException {
            final List<String> results = new ArrayList<>();
            for (final Map.Entry<String, T> entry : this.delegates.entrySet()) {
                final Collection<String> namespaces = entry.getValue().namespaces();
                for (final String namespace : namespaces) {
                    // attach scope to namespaces
                    final String scope = entry.getKey();
                    if (NamespaceableProvider.DEFAULT_SCOPE.equals(scope)) {
                        results.add(namespace);
                    } else {
                        results.add(String.format("%s.%s", scope, namespace));
                    }
                }
            }
            return results;
        }
    }

    static class ObjectsProxy extends NamespaceableProxy<Objects> {
        ObjectsProxy(final Map<String, Objects> delegates) {
            super(delegates);
        }
    }

    static class SetsProxy extends NamespaceableProxy<Sets> {
        SetsProxy(final Map<String, Sets> delegates) {
            super(delegates);
        }
    }

    static class EventsProxy extends NamespaceableProxy<Events> {
        EventsProxy(final Map<String, Events> delegates) {
            super(delegates);
        }
    }
}

