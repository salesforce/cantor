/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.common.providers;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.Namespaceable;
import com.salesforce.cantor.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class Factory {
    private static final Logger logger = LoggerFactory.getLogger(Factory.class);

    private final Map<String, ObjectsProvider> objects = new HashMap<>();
    private final Map<String, SetsProvider> sets = new HashMap<>();
    private final Map<String, EventsProvider> events = new HashMap<>();

    public Factory() {
        logger.info("cantor loading providers...");
        for (final ObjectsProvider provider : ServiceLoader.load(ObjectsProvider.class)) {
            logger.info("loading objects provider: '{}'", provider.getName());
            this.objects.put(provider.getName(), provider);
        }
        for (final SetsProvider provider : ServiceLoader.load(SetsProvider.class)) {
            logger.info("loading sets provider: '{}'", provider.getName());
            this.sets.put(provider.getName(), provider);
        }
        for (final EventsProvider provider : ServiceLoader.load(EventsProvider.class)) {
            logger.info("loading events provider: '{}'", provider.getName());
            this.events.put(provider.getName(), provider);
        }
        logger.info("cantor provider loaded '{}' objects, '{}' sets, '{}' events providers",
                this.objects.size(),
                this.sets.size(),
                this.events.size()
        );
    }

//    public Cantor get() {
//    }

    public static class CantorInvocationHandler implements InvocationHandler {
        private final Namespaceable target;

        public CantorInvocationHandler(final Events target) {
            this.target = target;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final Parameter[] parameters = method.getParameters();
            // there is only one method that takes no arguments, and that is 'namespaces()'
            if (parameters.length == 0 && method.getName().equals("namespaces")) {
                return method.invoke(this.target, args);
            }
            final String scopedNamespace = (String) args[0];
            if (!scopedNamespace.contains(".")) {
                // TODO this should be the default handler
                return method.invoke(this.target, args);
            }
            final String scope = scopedNamespace.substring(0, scopedNamespace.indexOf("."));
            final String namespace = scopedNamespace.substring(scopedNamespace.indexOf(".") + 1);
            // TODO find the handler for the given scope
            logger.info("this is the scope: {}", scope);
            logger.info("this is the namespace: {}", namespace);
            if (args.length == 1) {
                return method.invoke(this.target, namespace);
            }
            return method.invoke(this.target, namespace, Arrays.copyOfRange(args, 1, args.length));
        }
    }
}

