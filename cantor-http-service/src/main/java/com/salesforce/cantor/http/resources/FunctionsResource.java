/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

import com.google.gson.Gson;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.functions.Functions;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.*;

@Component
@Path("/functions")
@Tag(name = "Functions Resource", description = "Api for handling Cantor functions")
public class FunctionsResource {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();

    private final Cantor cantor;
    private final Functions functions;

    @Autowired
    public FunctionsResource(final Cantor cantor, final Functions functions) {
        this.cantor = cantor;
        this.functions = functions;
    }

    @PUT
    @Path("/{namespace}")
    @Operation(summary = "Create a new function namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Function namespace was created or already exists"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response createNamespace(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.functions.create(namespace);
        return Response.ok().build();
    }

    @DELETE
    @Path("/{namespace}")
    @Operation(summary = "Drop a function namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Function namespace was dropped or didn't exist"),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response dropNamespace(@Parameter(description = "Namespace identifier") @PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request to drop namespace {}", namespace);
        this.functions.drop(namespace);
        return Response.ok().build();
    }

    @GET
    @Path("/{namespace}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of all functions in the given namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all functions in the namespace",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunctions(@PathParam("namespace") final String namespace) throws IOException {
        logger.info("received request for all objects namespaces");
        return Response.ok(parser.toJson(this.functions.list(namespace))).build();
    }

    @GET
    @Path("/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the function with the given name",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunction(@PathParam("namespace") final String namespace,
                                @PathParam("function") final String functionName) throws IOException {
        final byte[] bytes = this.functions.get(namespace, functionName);
        if (bytes == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(bytes).build();
    }

    @GET
    @Path("/execute/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute get method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("namespace") final String namespace,
                                       @PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with get method", namespace, function);
        return executeFunction(namespace, function, request, response);
    }

    @PUT
    @Path("/execute/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute put method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response putExecuteFunction(@PathParam("namespace") final String namespace,
                                       @PathParam("function") final String function,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with put method", namespace, function);
        return executeFunction(namespace, function, request, response);
    }

    @POST
    @Path("/execute/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute post method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response postExecuteFunction(@PathParam("namespace") final String namespace,
                                        @PathParam("function") final String function,
                                        @Context final HttpServletRequest request,
                                        @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with post method", namespace, function);
        return executeFunction(namespace, function, request, response);
    }

    @DELETE
    @Path("/execute/{namespace}/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute delete method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteExecuteFunction(@PathParam("namespace") final String namespace,
                                          @PathParam("function") final String function,
                                          @Context final HttpServletRequest request,
                                          @Context final HttpServletResponse response) {
        logger.info("executing '{}/{}' with delete method", namespace, function);
        return executeFunction(namespace, function, request, response);
    }

    @PUT
    @Path("/{namespace}/{function}")
    @Operation(summary = "Store a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Function stored"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Namespace") @PathParam("namespace") final String namespace,
                           @Parameter(description = "Function identifier") @PathParam("function") final String functionName,
                           final String body) {
        try {
            this.functions.store(namespace, functionName, body);
            return Response.status(Response.Status.CREATED).build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    @DELETE
    @Path("/{namespace}/{function}")
    @Operation(summary = "Remove a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Function removed"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace") @PathParam("namespace") final String namespace,
                         @Parameter(description = "Namespace identifier") @PathParam("function") final String function) {
        try {
            this.functions.delete(namespace, function);
            return Response.ok().build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    private Response executeFunction(final String namespace,
                                     final String function,
                                     final HttpServletRequest request,
                                     final HttpServletResponse response) {
        try {
            final com.salesforce.cantor.functions.Context context =
                    new com.salesforce.cantor.functions.Context(this.cantor, this.functions);
            // special parameters, http.request and http.response are passed to functions
            context.set("http.request", request);
            context.set("http.response", response);
            this.functions.execute(namespace, function, context, getParams(request));

            // retrieve special parameter http.status from context
            final Object statusObject = context.get("http.status");
            final int status;
            if (statusObject instanceof String) {
                status = Integer.parseInt((String) statusObject);
            } else if (statusObject instanceof BigDecimal) {
                status = ((BigDecimal) statusObject).intValue();
            } else if (statusObject instanceof Long) {
                status = ((Long) statusObject).intValue();
            } else if (statusObject instanceof Integer) {
                status = (int) statusObject;
            } else {
                status = Response.Status.OK.getStatusCode();
            }
            final Response.ResponseBuilder builder = Response.status(status);
            // retrieve special parameter http.body from context
            if (context.get("http.body") != null) {
                builder.entity(context.get("http.body"));
            }
            // retrieve special parameter http.headers from context
            if (context.get("http.headers") != null) {
                for (final Map.Entry<String, Object> header : ((Map<String, Object>) context.get("http.headers")).entrySet()) {
                    builder.header(header.getKey(), header.getValue());
                }
            }
            return builder.build();
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }
    }

    private Map<String, String> getParams(final HttpServletRequest request) {
        final Map<String, String> params = new HashMap<>();
        for (final Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
            params.put(entry.getKey(), entry.getValue()[0]);
        }
        return params;
    }

    private String toString(final Throwable throwable) {
        final StringWriter writer = new StringWriter();
        final PrintWriter printer = new PrintWriter(writer);
        throwable.printStackTrace(printer);
        return writer.toString();
    }
}
