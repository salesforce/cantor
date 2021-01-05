package com.salesforce.cantor.misc.proxy;

import com.salesforce.cantor.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class CantorProxy implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public CantorProxy(final InvocationHandler objectsHandler,
                       final InvocationHandler setsHandler,
                       final InvocationHandler eventsHandler) {
        this.objects = (Objects) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] { Objects.class },
                objectsHandler
        );
        this.sets = (Sets) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] { Sets.class },
                setsHandler
        );
        this.events = (Events) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] { Events.class },
                eventsHandler
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
