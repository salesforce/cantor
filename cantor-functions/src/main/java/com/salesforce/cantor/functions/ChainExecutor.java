package com.salesforce.cantor.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The Chain Executor is used to execute a chain of functions. The chain is defined
 * as a '|' separated list of functions to execute. For example:
 *   "namespace1/func1?foo=bar | namespace2/func2 | namespace3/func3?bar=baz"
 *
 * Each function in the chain is executed in order, and the same context variable is
 * passed from one function call to the other, allowing functions to consume results from previous
 * functions in the chain.
 */
public class ChainExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(ChainExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("f", "chain");
    }

    @Override
    public void execute(final String function,
                        final byte[] body,
                        final Context context,
                        final Map<String, String> params) throws IOException {
        final String functionChain = new String(body, StandardCharsets.UTF_8);
        logger.info("executing function chain: {}", functionChain);

        // pipe is used to separate out function calls
        for (final String part : functionChain.split("\\|")) {
            final String qs = part.trim(); // remove spaces if any
            final String namespaceSlashFunction = qs.split("\\?")[0]; // anything before '?' in 'namespace/func?param=value'
            if (!namespaceSlashFunction.contains("/")) {
                throw new RuntimeException("invalid namespace/function format: " + namespaceSlashFunction);
            }
            final String functionNamespace = namespaceSlashFunction.split("/")[0]; // the namespace in 'namespace/func'
            final String functionName = namespaceSlashFunction.split("/")[1]; // the func in `namespace/func'
            final Map<String, String> functionParams = qs.contains("?")
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
                throw new RuntimeException("invalid key=value parameter format: " + kv);
            }
            params.put(keyValue[0], keyValue[1]);
        }
        return params;
    }
}
