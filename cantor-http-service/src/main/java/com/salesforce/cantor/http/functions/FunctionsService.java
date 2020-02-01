package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

@Service
public class FunctionsService {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsService.class);
    private static final String functorNamespace ="functor-functions";

    private final Cantor cantor;
    private final List<Executor> executors = new ArrayList<>();

    @Autowired
    public FunctionsService(final Cantor cantor) {
        this.cantor = cantor;
        this.executors.add(new FreemarkerExecutor());
        this.executors.add(new ScriptExecutor());

        try {
            this.cantor.objects().create(functorNamespace);
        } catch (IOException e) {
            logger.warn("failed to create functor namespace");
        }
    }

    public void storeFunction(final String functionName, final String functionBody)
            throws IOException {
        logger.info("storing new function with name '{}'", functionName);
        this.cantor.objects().store(functorNamespace, functionName, functionBody.getBytes(Charset.defaultCharset()));
    }

    public String getFunction(final String functionName) throws IOException {
        final byte[] functionBodyBytes = this.cantor.objects().get(functorNamespace, functionName);
        return functionBodyBytes != null ? new String(functionBodyBytes, Charset.defaultCharset()) : null;
    }

    public Collection<String> getFunctionsList() throws IOException {
        return this.cantor.objects().keys(functorNamespace, 0, -1);
    }

    public void execute(final String functionName, final Executor.Context context) throws IOException {
        final String functionBody = getFunction(functionName);
        if (functionBody == null) {
            throw new IllegalArgumentException("function not found: " + functionName);
        }
        // execute the function and pass context to it
        getExecutor(functionName).execute(functionName, functionBody, context);
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
}
