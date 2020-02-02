package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Executor {

    class Context {
        private final Map<String, Object> entities = new HashMap<>();
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final Cantor cantor;
        private int responseStatus = 0;
        private byte[] responseBody = null;
        private Map<String, String> responseHeaders = new HashMap<>();

        public Context(final HttpServletRequest request,
                       final HttpServletResponse response,
                       final Cantor cantor) {
            this.request = request;
            this.response = response;
            this.cantor = cantor;
        }

        public int getResponseStatus() {
            return this.responseStatus;
        }

        public byte[] getResponseBody() {
            return this.responseBody;
        }

        public void setResponseStatus(final int status) {
            this.responseStatus = status;
        }

        public void setResponseBody(final String body) {
            this.responseBody = body.getBytes();
        }

        public void setResponseBody(final byte[] body) {
            this.responseBody = body;
        }

        public HttpServletRequest getRequest() {
            return this.request;
        }

        public HttpServletResponse getResponse() {
            return this.response;
        }

        public void setResponseHeader(final String key, final String value) {
            this.responseHeaders.put(key, value);
        }

        public String getResponseHeader(final String key) {
            return this.responseHeaders.get(key);
        }

        public Map<String, String> getResponseHeaders() {
            return this.responseHeaders;
        }

        public Cantor getCantor() {
            return this.cantor;
        }

        public void set(final String key, final Object value) {
            this.entities.put(key, value);
        }

        public Object get(final String key) {
            return this.entities.get(key);
        }
    }

    // return list of file extensions to be handled by this executor
    List<String> getExtensions();

    // execute a function with the given parameters and return an entity as the response
    void execute(String functionName, String functionBody, Context context, Map<String, String> params);
}
