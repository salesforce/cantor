package com.salesforce.cantor.functions;

import com.salesforce.cantor.Cantor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Context {
    private final Cantor cantor;
    private final Functions functions;
    private final Map<String, Object> entities = new ConcurrentHashMap<>();

    public Context(final Cantor cantor, final Functions functions) {
        this.cantor = cantor;
        this.functions = functions;
    }

    public Cantor getCantor() {
        return this.cantor;
    }

    public Functions getFunctions() {
        return this.functions;
    }

    public void set(final String key, final Object value) {
        this.entities.put(key, value);
    }

    public Object get(final String key) {
        return this.entities.get(key);
    }

    public Set<String> keys() {
        return this.entities.keySet();
    }
}
