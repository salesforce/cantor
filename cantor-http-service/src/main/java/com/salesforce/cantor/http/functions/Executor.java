package com.salesforce.cantor.http.functions;

import java.util.List;
import java.util.Map;

public interface Executor {

    // return list of file extensions to be handled by this executor
    List<String> getExtensions();

    // execute a function with the given parameters and return an entity as the response
    Entity execute(String functionName, String functionBody, Map<String, String> parameters);
}
