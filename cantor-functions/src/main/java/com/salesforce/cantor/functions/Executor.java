package com.salesforce.cantor.functions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Executor {
    // return list of file extensions to be handled by this executor
    List<String> getExtensions();

    // execute a function with the given parameters and return an entity as the response
    void execute(String namespace, String function, byte[] body, Context context, Map<String, String> params)
            throws IOException;
}
