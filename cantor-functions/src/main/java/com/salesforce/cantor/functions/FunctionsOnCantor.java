/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.functions;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.*;

public class FunctionsOnCantor implements Functions {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsOnCantor.class);

    private static final String functionsNamespace = "functions";
    private static final String metadataKeyFunctionName = "function-name";

    private final Cantor cantor;
    private final List<Executor> executors = new ArrayList<>();

    public FunctionsOnCantor(final Cantor cantor) {
        this.cantor = cantor;
        initFunctionsNamespace();
        initExecutors();
    }

    @Override
    public void store(final String function, final String body) throws IOException {
        checkString(body, "missing function body");
        store(function, body.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void store(final String function, final byte[] body) throws IOException {
        checkString(function, "missing function name");
        checkArgument(function.contains("."), "missing extension in function name");
        checkArgument(body != null, "missing function body");
        doStore(function, body);
    }

    @Override
    public byte[] get(final String function) throws IOException {
        checkString(function, "missing function name");
        return doGet(function);
    }

    @Override
    public Collection<String> list() throws IOException {
        return doList();
    }

    @Override
    public void run(final String function,
                    final Context context,
                    final Map<String, String> params) throws IOException {
        checkString(function, "missing function name");
        checkArgument(context != null, "missing context");
        checkArgument(params != null, "missing parameters");
        doRun(function, context, params);
    }

    private void doRun(final String function,
                       final Context context,
                       final Map<String, String> params) throws IOException {
        final byte[] body = get(function);
        if (body == null) {
            throw new IllegalArgumentException("function not found: " + function);
        }
        // execute the function and pass context to it
        getExecutor(function).run(function, body, context, params);
    }

    // functions are stored as events in cantor, carrying the function name as metadata
    private void doStore(final String function, final byte[] body) throws IOException {
        logger.info("storing function: '{}'", function);
        final Map<String, String> metadata = new HashMap<>();
        metadata.put(metadataKeyFunctionName, function);
        this.cantor.events().store(functionsNamespace, System.currentTimeMillis(), metadata, null, body);
    }

    // retrieve the last version of a function
    private byte[] doGet(final String function) throws IOException {
        logger.info("retrieving function: '{}'", function);
        final Map<String, String> metadataQuery = new HashMap<>();
        metadataQuery.put(metadataKeyFunctionName, function);
        final Events.Event functionEvent = this.cantor.events().last(functionsNamespace, 0, Long.MAX_VALUE, metadataQuery, null, true);
        return functionEvent == null ? null : functionEvent.getPayload();
    }

    private Collection<String> doList() throws IOException {
        return this.cantor.events().metadata(
                functionsNamespace, metadataKeyFunctionName, 0, Long.MAX_VALUE, null, null
        );
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

    private void initExecutors() {
        logger.info("loading all executors available in class path");
        final ServiceLoader<Executor> loader = ServiceLoader.load(Executor.class);
        for (final Executor executor : loader) {
            logger.info("loading executor: {} for extensions: {}", executor.getClass().getSimpleName(), executor.getExtensions());
            this.executors.add(executor);
        }
    }

    private void initFunctionsNamespace() {
        logger.info("initializing functions namespace");
        try {
            this.cantor.events().create(functionsNamespace);
        } catch (IOException e) {
            logger.error("failed to initialize functions namespace; rethrowing as runtime exception", e);
            throw new RuntimeException(e);
        }
    }

    private String getExtension(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }
}
