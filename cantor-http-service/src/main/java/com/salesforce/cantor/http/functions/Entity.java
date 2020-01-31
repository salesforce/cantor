package com.salesforce.cantor.http.functions;

import java.util.Map;

public class Entity {
    private int status;
    private Map<String, String> headersMap;
    private byte[] body;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Map<String, String> getHeadersMap() {
        return headersMap;
    }

    public void setHeadersMap(Map<String, String> headersMap) {
        this.headersMap = headersMap;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
