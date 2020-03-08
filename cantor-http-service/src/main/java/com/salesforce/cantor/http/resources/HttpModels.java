/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

import com.salesforce.cantor.Events;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Base64;
import java.util.Map;

/**
 * Swagger api documentation requires these classes to display json models of the responses.<br/>
 * Each class's javadoc is what Swagger will translate them into on the ui.
 */
public class HttpModels {
    /**
     * { "results": true }
     */
    @Schema
    static class DeleteResponse {
        @Schema(description = "boolean success state")
        public boolean getResults() {
            return true;
        }
    }

    /**
     * { "size": 0 }
     */
    @Schema
    static class SizeResponse {
        @Schema(description = "total number of elements found")
        public int getSize() {
            return 0;
        }
    }

    /**
     * { "count": 0 }
     */
    @Schema
    static class CountResponse {
        @Schema(description = "total number of elements successfully executed on")
        public int getCount() {
            return 0;
        }
    }

    /**
     * { "weight": 0 }
     */
    @Schema
    static class WeightResponse {
        @Schema(description = "weight of the specified entry")
        public long getWeight() {
            return 0;
        }
    }

    /**
     * { "data": "string" }
     */
    @Schema
    static class GetResponse {
        @Schema(description = "base64 encoded string payload of requested entry")
        public String getData() {
            return "";
        }
    }

    /**
     * {
     *   "timestampMillis": 0,
     *   "metadata": {},
     *   "dimensions": {},
     *   "payload": "base64 representation of bytes"
     * }
     */
    @Schema
    static class EventModel {
        @Schema(description = "Timestamp of the event", required = true)
        private long timestampMillis;
        @Schema(description = "Metadata - string to string key/value pairs")
        private Map<String, String> metadata;
        @Schema(description = "Dimensions - string to double key/value pairs")
        private Map<String, Double> dimensions;
        @Schema(description = "Base64 encoded string payload", example = "QmFzZTY0IGVuY29kZWQ=")
        private String payload;

        public long getTimestampMillis() { return this.timestampMillis; }
        public Map<String, String> getMetadata() { return this.metadata; }
        public Map<String, Double> getDimensions() { return this.dimensions; }
        public String getPayload() { return this.payload; }

        public Events.Event toCantorEvent() {
            return new Events.Event(this.timestampMillis,
                    this.metadata,
                    this.dimensions,
                    Base64.getDecoder().decode(this.payload));
        }
    }
}
