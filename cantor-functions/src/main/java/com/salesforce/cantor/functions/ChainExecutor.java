package com.salesforce.cantor.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ChainExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(ChainExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("f", "chain");
    }

    @Override
    public void execute(final String namespace,
                        final String function,
                        final byte[] body,
                        final Context context,
                        final Map<String, String> params)
            throws IOException {
        final String functionChain = new String(body, StandardCharsets.UTF_8);
        logger.info("executing function chain: {}", functionChain);

        // pipe is used to separate out function calls
        for (final String qs : functionChain.split("\\|")) {
            // before ? is namespace/function
            final String namespaceSlashFunction = qs.split("\\?")[0];
            final String functionNamespace = namespaceSlashFunction.split("/")[0];
            final String functionName = namespaceSlashFunction.split("/")[1];
            final Map<String, String> functionParams = qs.contains("&")
                    ? parseParams(qs.substring(qs.indexOf("?") + 1))
                    : Collections.emptyMap();
            logger.info("executing function '{}' with parameters: '{}'", functionName, functionParams);
            context.getFunctions().execute(functionNamespace, functionName, context, functionParams);
            logger.info("context: {}", context.keys());
        }
    }

    private Map<String, String> parseParams(final String filterQueryString) {
        final Map<String, String> params = new HashMap<>();
        for (final String kv : filterQueryString.split("&")) {
            final String[] keyValue = kv.split("=");
            if (keyValue.length == 1) {
                continue;
            }
            params.put(keyValue[0], keyValue[1]);
        }
        return params;
    }
}
