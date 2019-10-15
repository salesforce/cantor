/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.jersey;

import com.google.gson.Gson;
import com.salesforce.cantor.Cantor;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.BeanParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Path("/sets")
@Tag(name = "Sets Resource", description = "Api for handling Cantor Sets")
public class SetsResource {
    private static final Logger logger = LoggerFactory.getLogger(SetsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();
    private static final String jsonFieldResults = "results";
    private static final String jsonFieldSize = "size";
    private static final String jsonFieldWeight = "weight";

    private final Cantor cantor;

    @Autowired
    public SetsResource(final Cantor cantor) {
        this.cantor = cantor;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get all sets namespaces")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all namespaces",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getNamespaces() throws IOException {
        logger.info("received request for all sets namespaces");
        return Response.ok(parser.toJson(this.cantor.sets().namespaces())).build();
    }

    @PUT
    @Path("/{namespace}")
    @Operation(summary = "Create a new set namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Set namespace was created or already existed"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request for creation of namespace {}", namespace);
        this.cantor.sets().create(namespace);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}")
    @Operation(summary = "Drop a set namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Set namespace was dropped or didn't exist"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.cantor.sets().drop(namespace);
        return Response.ok().build();
    }

    @PUT
    @Path("/{namespace}/{set}/{entry}/{weight}")
    @Operation(summary = "Add or overwrite an entry in a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Entry was successfully added or its weight was updated"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response add(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                        @Parameter(description = "Name of the set") @PathParam("set") final String set,
                        @Parameter(description = "Name of the entry") @PathParam("entry") final String entry,
                        @Parameter(description = "Weight of the entry") @PathParam("weight") final long weight) throws IOException {
        logger.info("received request to add entry with weight {}:{} in set/namespace {}/{}", entry, weight, set, namespace);
        this.cantor.sets().add(namespace, set, entry, weight);
        return Response.ok().build();
    }

    @GET
    @Path("/entries/{namespace}/{set}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get entry names from a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides entry names matching query parameters as a list",
                     content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response entries(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                            @Parameter(description = "Name of the set") @PathParam("set") final String set,
                            @BeanParam final SetsDataSourceBean bean) throws IOException {
        logger.info("received request for entries in set/namespace {}/{}", set, namespace);
        logger.debug("request parameters: {}", bean);
        final Collection<String> entries = this.cantor.sets().entries(
                namespace,
                set,
                bean.getMin(),
                bean.getMax(),
                bean.getStart(),
                bean.getCount(),
                bean.isAscending());
        return Response.ok(parser.toJson(entries)).build();
    }

    @GET
    @Path("/{namespace}/{set}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get entries from a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides entry names and weights matching query parameters as properties in a json",
                     content = @Content(schema = @Schema(implementation = Map.class))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response get(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                        @Parameter(description = "Name of the set") @PathParam("set") final String set,
                        @BeanParam final SetsDataSourceBean bean) throws IOException {
        logger.info("received request for values in set/namespace {}/{}", set, namespace);
        logger.debug("request parameters: {}", bean);
        final Map<String, Long> entries = this.cantor.sets().get(
                namespace,
                set,
                bean.getMin(),
                bean.getMax(),
                bean.getStart(),
                bean.getCount(),
                bean.isAscending());
        return Response.ok(parser.toJson(entries)).build();
    }

    @DELETE
    @Path("/{namespace}/{set}")
    @Operation(summary = "Delete entries in a set between provided weights")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully deleted entries between and including provided weights"),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response delete(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Name of the set") @PathParam("set") final String set,
                           @Parameter(description = "Minimum weight for an entry", example = "0") @QueryParam("min") final long min,
                           @Parameter(description = "Maximum weight for an entry", example = "0") @QueryParam("max") final long max) throws IOException {
        logger.info("received request to delete entries in set/namespace {}/{} between weights {}-{}", set, namespace, min, max);
        this.cantor.sets().delete(namespace, set, min, max);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}/{set}/{entry}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete a specific entry by name")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with a boolean which is only true if the key was found and the entry was deleted",
                     content = @Content(schema = @Schema(implementation = HttpModels.DeleteResponse.class))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response delete(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Name of the set") @PathParam("set") final String set,
                           @Parameter(description = "Name of the entry") @PathParam("entry") final String entry) throws IOException {
        logger.info("received request to delete entry {} in set/namespace {}/{}", entry, set, namespace);
        final Map<String, Boolean> completed = new HashMap<>();
        completed.put(jsonFieldResults, this.cantor.sets().delete(namespace, set, entry));
        return Response.ok(parser.toJson(completed)).build();
    }

    @GET
    @Path("/union/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Perform a union of all provided sets")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides the union of all entries filtered by query parameters as properties in a json",
                     content = @Content(schema = @Schema(implementation = Map.class))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response union(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                          @Parameter(description = "Name of the set") @QueryParam("set") final List<String> sets,
                          @BeanParam final SetsDataSourceBean bean) throws IOException {
        logger.info("received request for union of sets {} in namespace {}", sets, namespace);
        logger.debug("request parameters: {}", bean);
        final Map<String, Long> union = this.cantor.sets().union(
                namespace,
                sets,
                bean.getMin(),
                bean.getMax(),
                bean.getStart(),
                bean.getCount(),
                bean.isAscending());
        return Response.ok(parser.toJson(union)).build();
    }

    @GET
    @Path("/intersect/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Perform an intersection of all provided sets")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides an intersection of all entries filtered by query parameters as properties in a json",
                     content = @Content(schema = @Schema(implementation = Map.class))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response intersect(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                              @Parameter(description = "List of sets") @QueryParam("set") final List<String> sets,
                              @BeanParam final SetsDataSourceBean bean) throws IOException {
        logger.info("received request for intersection of sets {} in namespace {}", sets, namespace);
        logger.debug("request parameters: {}", bean);
        final Map<String, Long> intersection = this.cantor.sets().intersect(
                namespace,
                sets,
                bean.getMin(),
                bean.getMax(),
                bean.getStart(),
                bean.getCount(),
                bean.isAscending());
        return Response.ok(parser.toJson(intersection)).build();
    }

    @DELETE
    @Path("/pop/{namespace}/{set}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Pop entries from a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Entries and weights of elements popped matching query parameters",
                     content = @Content(schema = @Schema(implementation = Map.class))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response pop(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                        @Parameter(description = "Name of the set") @PathParam("set") final String set,
                        @BeanParam final SetsDataSourceBean bean) throws IOException {
        logger.info("received request to pop off set/namespace {}/{}", set, namespace);
        logger.debug("request parameters: {}", bean);
        final Map<String, Long> entries = this.cantor.sets().pop(
                namespace,
                set,
                bean.getMin(),
                bean.getMax(),
                bean.getStart(),
                bean.getCount(),
                bean.isAscending());
        return Response.ok(parser.toJson(entries)).build();
    }

    @GET
    @Path("/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of all sets in a namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides a list of all sets in namespace",
                     content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response sets(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request for all sets in namespace {}", namespace);
        return Response.ok(parser.toJson(this.cantor.sets().sets(namespace))).build();
    }

    @GET
    @Path("/size/{namespace}/{set}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get number of entries in a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with the size of the set",
                     content = @Content(schema = @Schema(implementation = HttpModels.SizeResponse.class))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response size(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                         @Parameter(description = "Name of the set") @PathParam("set") final String set) throws IOException {
        logger.info("received request for size of set/namespace {}/{}", set, namespace);
        final Map<String, Integer> size = new HashMap<>();
        size.put(jsonFieldSize, this.cantor.sets().size(namespace, set));
        return Response.ok(parser.toJson(size)).build();
    }

    @GET
    @Path("/weight/{namespace}/{set}/{entry}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get weight of a specific entry in a set")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with the weight of the entry if it exists",
                     content = @Content(schema = @Schema(implementation = HttpModels.WeightResponse.class))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response weight(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Name of the set") @PathParam("set") final String set,
                           @Parameter(description = "Name of the entry") @PathParam("entry") final String entry) throws IOException {
        logger.info("received request for weight of entry {} in set/namespace {}/{}", entry, set, namespace);
        final Map<String, Long> weight = new HashMap<>();
        weight.put(jsonFieldWeight, this.cantor.sets().weight(namespace, set, entry));
        return Response.ok(parser.toJson(weight)).build();
    }

    @POST
    @Path("/{namespace}/{set}/{entry}/{count}")
    @Operation(summary = "Increment a specific entry")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully incremented entry by count or the entry didn't exist"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response inc(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                        @Parameter(description = "Name of the set") @PathParam("set") final String set,
                        @Parameter(description = "Name of the entry") @PathParam("entry") final String entry,
                        @Parameter(description = "Amount to increment", example = "10") @PathParam("count") final long count) throws IOException {
        logger.info("received request to increment entry {} in set/namespace {}/{} by {}", entry, set, namespace, count);
        this.cantor.sets().inc(namespace, set, entry, count);
        return Response.ok().build();
    }

    protected static class SetsDataSourceBean {
        @Parameter(description = "Minimum weight for an entry", example = "0")
        @QueryParam("min")
        private long min;

        @Parameter(description = "Maximum weight for an entry", example = "-1")
        @QueryParam("max")
        private long max;

        @Parameter(description = "Index from which to start counting", example = "0")
        @QueryParam("start")
        private int start;

        @Parameter(description = "Number of entries allowed in response", example = "10")
        @QueryParam("count")
        private int count;

        @Parameter(description = "Return in ascending or descending format", example = "false")
        @QueryParam("asc")
        private boolean ascending;

        /*
         * Getters and setter are required for Swagger to process the Jersey bean
         * Swagger does not currently support the BeanParam annotation with a constructor
         */

        public long getMin() {
            return min;
        }

        public long getMax() {
            if (this.max == -1) {
                this.max = Long.MAX_VALUE;
                logger.info("setting bean max to Long.MAX_VALUE");
            }
            return max;
        }

        public int getStart() {
            return start;
        }

        public int getCount() {
            return count;
        }

        public boolean isAscending() {
            return ascending;
        }

        public void setMin(final long min) {
            this.min = min;
        }

        public void setMax(final long max) {
            this.max = max;
        }

        public void setStart(final int start) {
            this.start = start;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        public void setAscending(final boolean ascending) {
            this.ascending = ascending;
        }
    }
}
