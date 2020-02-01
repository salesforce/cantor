package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

public interface Executor {

    class Context {
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final Cantor cantor;
        private final Entity entity;
        private final Map<String, String> params;

        public Context(final HttpServletRequest request,
                       final HttpServletResponse response,
                       final Cantor cantor,
                       final Entity entity, final Map<String, String> params) {
            this.request = request;
            this.response = response;
            this.cantor = cantor;
            this.entity = entity;
            this.params = params;
        }

        public HttpServletRequest getRequest() {
            return this.request;
        }

        public HttpServletResponse getResponse() {
            return this.response;
        }
        public Cantor getCantor() {
            return this.cantor;
        }

        public Entity getEntity() {
            return this.entity;
        }

        public Map<String, String> getParams() {
            return this.params;
        }
    }

    // return list of file extensions to be handled by this executor
    List<String> getExtensions();

    // execute a function with the given parameters and return an entity as the response
    void execute(String functionName, String functionBody, Context context);
}
