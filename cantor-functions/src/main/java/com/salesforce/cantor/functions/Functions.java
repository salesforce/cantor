package com.salesforce.cantor.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Functions {
    private static final Logger logger = LoggerFactory.getLogger(Functions.class);

    private final Cantor cantor;
    private final List<Executor> executors = new ArrayList<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public Functions(final Cantor cantor) {
        this.cantor = cantor;
        initExecutors();
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

    private void initExecutors() {
        final ServiceLoader<Executor> loader = ServiceLoader.load(Executor.class);
        for (final Executor executor : loader) {
            logger.info("loading function executor: {} for extensions: {}",
                    executor.getClass().getSimpleName(), executor.getExtensions());
            this.executors.add(executor);
        }
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
}
