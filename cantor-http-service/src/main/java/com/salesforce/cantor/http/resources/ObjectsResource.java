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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
@Path("/objects")
@Tag(name = "Objects Resource", description = "Api for handling Cantor Objects")
public class ObjectsResource {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();
    private static final String jsonFieldData = "data";
    private static final String jsonFieldResults = "results";
    private static final String jsonFieldSize = "size";

    private final Cantor cantor;

    @Autowired
    public ObjectsResource(final Cantor cantor) {
        this.cantor = cantor;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get all objects namespaces")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all namespaces",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getNamespaces() throws IOException {
        logger.info("received request for all objects namespaces");
        return Response.ok(parser.toJson(this.cantor.objects().namespaces())).build();
    }

    @PUT
    @Path("/{namespace}")
    @Operation(summary = "Create a new object namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Object namespace was created or already existed"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to create namespace {}", namespace);
        this.cantor.objects().create(namespace);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}")
    @Operation(summary = "Drop an object namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Object namespace was dropped or didn't exist"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.cantor.objects().drop(namespace);
        return Response.ok().build();
    }

    @PUT
    @Path("/{namespace}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_FORM_URLENCODED, MediaType.MULTIPART_FORM_DATA, MediaType.TEXT_PLAIN})
    @Operation(summary = "Add or overwrite an object in a namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Object was added or existing content was overwritten"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response store(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                          @Parameter(description = "Key of the object") @PathParam("key") final String key,
                          @Parameter(description = "Content of the object") final byte[] bytes) throws IOException {
        logger.info("received request to store object with key '{}' in namespace {}", key, namespace);
        logger.debug("object bytes: {}", bytes);
        this.cantor.objects().store(namespace, key, bytes);
        return Response.ok().build();
    }

    @GET
    @Path("/{namespace}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get an object's content by its key")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with the object content as base64 encoded string",
                     content = @Content(schema = @Schema(implementation = HttpModels.GetResponse.class))),
        @ApiResponse(responseCode = "204", description = "Object exists, but has no content"),
        @ApiResponse(responseCode = "404", description = "Object with provided key doesn't exist"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getByKey(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                             @Parameter(description = "Key of the object") @PathParam("key") final String key) throws IOException {
        logger.info("received request to get object with key '{}' in namespace {}", key, namespace);
        final byte[] bytes = this.cantor.objects().get(namespace, key);
        if (bytes == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (bytes.length == 0) {
            return Response.noContent().build();
        }

        final String encodedData = Base64.getEncoder().encodeToString(bytes);
        final Map<String, String> data = new HashMap<>();
        data.put(jsonFieldData, encodedData);
        return Response.ok(parser.toJson(data)).build();
    }

    @DELETE
    @Path("/{namespace}/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete an object by its key")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with a boolean which is only true if the key was found and the object was deleted",
                     content = @Content(schema = @Schema(implementation = HttpModels.DeleteResponse.class))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteByKey(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                                @Parameter(description = "Key of the object") @PathParam("key") final String key) throws IOException {
        logger.info("received request to delete object '{}' in namespace {}", key, namespace);
        final Map<String, Boolean> completed = new HashMap<>();
        completed.put(jsonFieldResults, this.cantor.objects().delete(namespace, key));
        return Response.ok(parser.toJson(completed)).build();
    }

    @GET
    @Path("/keys/{namespace}/{prefix}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the keys matching prefix for objects in a namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides a list of keys in namespace",
                     content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response keys(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                         @Parameter(description = "Prefix all returned keys will match") @PathParam("prefix") final String prefix,
                         @Parameter(description = "Index from which to start counting") @QueryParam("start") final int start,
                         @Parameter(description = "Number of entries allowed in response", example = "10") @QueryParam("count") final int count) throws IOException {
        logger.info("received request to get keys {}-{} in namespace {} with prefix {}", start, start + count, namespace, prefix);
        final Collection<String> keys = this.cantor.objects().keys(namespace, prefix, start, count);
        return Response.ok(parser.toJson(keys)).build();
    }

    @GET
    @Path("/keys/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get the keys of objects in a namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides a list of keys in namespace",
                     content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
        @ApiResponse(responseCode = "400", description = "One of the query parameters has a bad value"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response keys(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace,
                         @Parameter(description = "Index from which to start counting") @QueryParam("start") final int start,
                         @Parameter(description = "Number of entries allowed in response", example = "10") @QueryParam("count") final int count) throws IOException {
        logger.info("received request to get keys {}-{} in namespace {}", start, start + count, namespace);
        final Collection<String> keys = this.cantor.objects().keys(namespace, start, count);
        return Response.ok(parser.toJson(keys)).build();
    }

    @GET
    @Path("/size/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "View size of a namespace")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
                     description = "Provides single property json with the number of objects in a namespace",
                     content = @Content(schema = @Schema(implementation = HttpModels.SizeResponse.class))),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response size(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to get size of namespace {}", namespace);
        final Map<String, Integer> completed = new HashMap<>();
        completed.put(jsonFieldSize, this.cantor.objects().size(namespace));
        return Response.ok(parser.toJson(completed)).build();
    }
}
