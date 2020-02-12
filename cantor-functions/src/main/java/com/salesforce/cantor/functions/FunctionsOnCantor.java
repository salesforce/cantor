package com.salesforce.cantor.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.salesforce.cantor.common.CommonPreconditions.*;

public class FunctionsOnCantor implements Functions {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsOnCantor.class);

    private final Cantor cantor;
    private final List<Executor> executors = new ArrayList<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public FunctionsOnCantor(final Cantor cantor) {
        this.cantor = cantor;
        initExecutors();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkNamespace(namespace);
        doCreate(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkNamespace(namespace);
        doDrop(namespace);
    }

    @Override
    public void store(final String namespace, final String function, final String body) throws IOException {
        checkString(body, "missing function body");
        store(namespace, function, body.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void store(final String namespace, final String function, final byte[] body) throws IOException {
        checkNamespace(namespace);
        checkString(function, "missing function name");
        checkArgument(function.contains("."), "missing extension in function name");
        checkArgument(body != null, "missing function body");
        doStore(namespace, function, body);
    }

    @Override
    public byte[] get(final String namespace, final String function) throws IOException {
        checkNamespace(namespace);
        checkString(function, "missing function name");
        return doGet(namespace, function);
    }

    @Override
    public void delete(final String namespace, final String function) throws IOException {
        checkNamespace(namespace);
        checkString(function, "missing function name");
        doDelete(namespace, function);
    }

    @Override
    public Collection<String> list(final String namespace) throws IOException {
        checkNamespace(namespace);
        return doList(namespace);
    }

    @Override
    public void run(final String namespace,
                    final String function,
                    final Context context,
                    final Map<String, String> params) throws IOException {
        checkNamespace(namespace);
        checkString(function, "missing function name");
        checkArgument(context != null, "missing context");
        checkArgument(params != null, "missing parameters");
        doRun(namespace, function, context, params);
    }

    private void doRun(final String namespace,
                       final String function,
                       final Context context,
                       final Map<String, String> params) throws IOException {
        final byte[] body = get(namespace, function);
        if (body == null) {
            throw new IllegalArgumentException("function not found: " + function);
        }
        // execute the function and pass context to it
        getExecutor(function).run(function, body, context, params);
    }

    private void initExecutors() {
        logger.info("loading all executors available in class path");
        final ServiceLoader<Executor> loader = ServiceLoader.load(Executor.class);
        for (final Executor executor : loader) {
            logger.info("loading executor: {} for extensions: {}", executor.getClass().getSimpleName(), executor.getExtensions());
            this.executors.add(executor);
        }
    }

    private void doCreate(final String namespace) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("creating objects namespace for functions: '{}'", functionNamespace);
        this.cantor.objects().create(functionNamespace);
    }

    private void doDrop(final String namespace) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("dropping objects namespace for functions: '{}'", functionNamespace);
        this.cantor.objects().drop(functionNamespace);
    }

    private void doStore(final String namespace, final String function, final byte[] body) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("storing function: '{}' in objects namespace: '{}'", function, functionNamespace);
        this.cantor.objects().store(functionNamespace, function, body);
    }

    private byte[] doGet(final String namespace, final String function) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("retrieving function: '{}' from objects namespace: '{}'", function, functionNamespace);
        return this.cantor.objects().get(functionNamespace, function);
    }

    private void doDelete(final String namespace, final String function) throws IOException {
        final String functionNamespace = getFunctionNamespace(namespace);
        logger.info("deleting function: name '{}' from objects namespace: '{}'", function, functionNamespace);
        this.cantor.objects().delete(functionNamespace, function);
    }

    private Collection<String> doList(final String namespace) throws IOException {
        return this.cantor.objects().keys(getFunctionNamespace(namespace), 0, -1);
    }

    private Executor getExecutor(final String functionName) {
        // find the executor that support this function extension
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
