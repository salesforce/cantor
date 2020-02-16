package com.salesforce.cantor.functions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Executor {

    /**
     * Returns list of extensions this executor can execute. For example, a Python executor may return .py and .python.
     *
     * @return list of acceptable extensions
     */
    List<String> getExtensions();

    /***
     * Given the body of a function which its name ends with an extension that this executor accepts, run the function.
     *
     * @param function the function name
     * @param body the body of the function
     * @param context context variable
     * @param params parameters passed for this execution
     */
    void run(String function, byte[] body, Context context, Map<String, String> params) throws IOException;
}
