package com.salesforce.cantor.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Functions {
    private static final Logger logger = LoggerFactory.getLogger(Functions.class);

    private final Cantor cantor;
    private final List<Executor> executors = new ArrayList<>();

    public interface Executor {

        // return list of file extensions to be handled by this executor
        List<String> getExtensions();

        // execute a function with the given parameters and return an entity as the response
        void execute(String function, byte[] body, Context context, Map<String, String> params) throws IOException;
    }

    public Functions(final Cantor cantor) {
        this.cantor = cantor;
        this.executors.add(new FreemarkerExecutor());
        this.executors.add(new ScriptExecutor());
        this.executors.add(new ChainExecutor());
        this.executors.add(new JavaExecutor());
    }

    public void create(final String namespace) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("creating objects namespace for functions: '{}'", functionNamespace);
        this.cantor.objects().create(functionNamespace);
    }

    public void drop(final String namespace) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("dropping objects namespace for functions: '{}'", functionNamespace);
        this.cantor.objects().drop(functionNamespace);
    }

    public void store(final String namespace, final String function, final String body) throws IOException {
        store(namespace, function, body.getBytes(StandardCharsets.UTF_8));
    }

    public void store(final String namespace, final String function, final byte[] body) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("storing function: '{}' in objects namespace: '{}'", function, functionNamespace);
        this.cantor.objects().store(functionNamespace, function, body);
    }

    public byte[] get(final String namespace, final String function) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("retrieving function: '{}' from objects namespace: '{}'", function, functionNamespace);
        return this.cantor.objects().get(functionNamespace, function);
    }

    public void delete(final String namespace, final String function) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("deleting function: name '{}' from objects namespace: '{}'", function, functionNamespace);
        this.cantor.objects().delete(functionNamespace, function);
    }

    public Collection<String> list(final String namespace) throws IOException {
        return this.cantor.objects().keys(getFunctionNamespace(namespace), 0, -1);
    }

    public void execute(final String namespace,
                        final String function,
                        final Context context,
                        final Map<String, String> params) throws IOException {
        final byte[] body = get(namespace, function);
        if (body == null) {
            throw new IllegalArgumentException("function not found: " + function);
        }
        // execute the function and pass context to it
        getExecutor(function).execute(function, body, context, params);
    }

    // return the executor instance for the given executor name
    private Executor getExecutor(final String functionName) {
        final String extension = getExtension(functionName);
        for (final Executor executor : this.executors) {
            if (executor.getExtensions().contains(extension)) {
                return executor;
            }
        }
        final List<String> extensions = new ArrayList<>();
        for (final Executor executor : this.executors) {
            extensions.addAll(executor.getExtensions());
        }
        throw new IllegalArgumentException(
                "executor for extension '" + extension + "' not found; supported extensions are: " + extensions.toString()
        );
    }

    private String getExtension(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }

    private String getFunctionNamespace(final String namespace) {
        return String.format("functions-%s", namespace);
    }

    public static class Context {
        private final Map<String, Object> entities = new ConcurrentHashMap<>();
        private final Cantor cantor;
        private final Functions functions;

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
}
