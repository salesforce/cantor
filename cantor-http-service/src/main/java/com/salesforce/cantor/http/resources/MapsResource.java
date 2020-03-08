/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

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

import javax.ws.rs.Consumes;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Path("/maps")
@Tag(name = "Maps Resource", description = "Api for handling Cantor Maps")
public class MapsResource {
    private static final Logger logger = LoggerFactory.getLogger(EventsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";
    private static final String jsonFieldCount = "count";
    private static final Gson parser = new Gson();

    private final Cantor cantor;

    @Autowired
    public MapsResource(final Cantor cantor) {
        this.cantor = cantor;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get all maps namespaces")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all namespaces",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getNamespaces() throws IOException {
        logger.info("received request for all maps namespaces");
        return Response.ok(parser.toJson(this.cantor.maps().namespaces())).build();
    }


    @PUT
    @Path("/{namespace}")
    @Operation(summary = "Create a new maps namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Map namespace was created or already existed"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to create namespace {}", namespace);
        this.cantor.maps().create(namespace);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}")
    @Operation(summary = "Drop an map namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Map namespace was dropped or didn't exist"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.cantor.maps().drop(namespace);
        return Response.ok().build();
    }

    @POST
    @Path("/{namespace}/store")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Add an immutable map to a namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Map was added"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response store(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                          @Parameter(description = "Map content to be stored") final Map<String, String> map) throws IOException {
        logger.info("received request to add map to namespace {}", namespace);
        this.cantor.maps().store(namespace, map);
        return Response.ok().build();
    }

    @GET
    @Path("/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get all maps containing query map")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "List of all maps matching query map",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = Map.class, type = "object")))),
            @ApiResponse(responseCode = "400", description = "One of the query parameters was malformed"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response get(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                        @Parameter(description = "Generic parameters expected to be key value pairs in the format: <key>=<value> (spaces will be trimmed)") @QueryParam("query") final List<String> queryMapAsList) throws IOException {
        logger.info("received request to retrieve maps in namespace {}", namespace);
        final Collection<Map<String, String>> resultMaps = this.cantor.maps().get(namespace, queryToMap(queryMapAsList));
        return Response.ok(parser.toJson(resultMaps)).build();
    }

    @DELETE
    @Path("/{namespace}/delete")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete all maps containing query map")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides single property json with an integer specifying the number of maps deleted",
                    content = @Content(schema = @Schema(implementation = HttpModels.CountResponse.class))),
            @ApiResponse(responseCode = "400", description = "One of the query parameters was malformed"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response delete(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Generic parameters expected to be key value pairs in the format: <key>=<value> (spaces will be trimmed)") @QueryParam("query") final List<String> queryMapAsList) throws IOException {
        logger.info("received request to delete maps in namespace {}", namespace);
        final Map<String, Integer> deleted = new HashMap<>();
        deleted.put(jsonFieldCount, this.cantor.maps().delete(namespace, queryToMap(queryMapAsList)));
        return Response.ok(parser.toJson(deleted)).build();
    }

    private static Map<String, String> queryToMap(final List<String> queryList) {
        if (queryList.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> queryMap = new HashMap<>();
        for (final String query : queryList) {
            if (query == null || query.isEmpty()) {
                continue;
            }

            final String[] keyValuePair = query.split("=");
            if (keyValuePair.length == 2) {
                queryMap.put(keyValuePair[0].trim(), keyValuePair[1].trim());
            } else {
                throw new IllegalArgumentException("Invalid query format: " + query);
            }
        }
        return queryMap;
    }
}
