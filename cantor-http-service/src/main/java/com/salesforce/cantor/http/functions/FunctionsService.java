package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

@Service
public class FunctionsService {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsService.class);
    private static final String functorNamespace ="functor-functions";

    private final Cantor cantor;
    private final ExecutorsService executorsService;

    @Autowired
    public FunctionsService(final Cantor cantor, final ExecutorsService executorsService) {
        this.cantor = cantor;
        this.executorsService = executorsService;

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

    public Entity execute(final String functionName,
                          final Map<String, String> params) throws IOException {

        final String functionBody = getFunction(functionName);
        if (functionBody == null) {
            throw new IllegalArgumentException("function not found");
        }

        final Executor executor = this.executorsService.getExecutor(functionName);
        return executor.execute(functionName, functionBody, params);
    }
}
