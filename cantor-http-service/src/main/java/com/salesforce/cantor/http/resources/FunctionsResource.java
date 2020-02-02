/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.resources;

import com.google.gson.Gson;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.http.functions.*;
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
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

@Component
@Path("/functions")
@Tag(name = "Functions Resource", description = "Api for handling Cantor functions")
public class FunctionsResource {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();

    private final Cantor cantor;
    private final FunctionsService functionsService;

    @Autowired
    public FunctionsResource(final Cantor cantor) {
        this.cantor = cantor;
        this.functionsService = new FunctionsService(cantor);
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
        this.functionsService.createNamespace(namespace);
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
        this.functionsService.dropNamespace(namespace);
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
        return Response.ok(parser.toJson(this.functionsService.getFunctionsList(namespace))).build();
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
        final String functionBody = this.functionsService.getFunction(namespace, functionName);
        // add all headers from the result
        return functionBody != null
                ? Response.ok(functionBody).header("Content-Type", MediaType.TEXT_PLAIN).build()
                : Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("/execute/{function:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute get method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("function") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @PUT
    @Path("/execute/{function:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute put method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response putExecuteFunction(@PathParam("function") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @POST
    @Path("/execute/{function:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute post method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response postExecuteFunction(@PathParam("function") final String ignored,
                                        @Context final HttpServletRequest request,
                                        @Context final HttpServletResponse response,
                                        @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @DELETE
    @Path("/execute/{function:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute delete method on function query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the function query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteExecuteFunction(@PathParam("function") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
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
            this.functionsService.storeFunction(namespace, functionName, body);
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
            this.functionsService.deleteFunction(namespace, function);
            return Response.ok().build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    private Response executeFunctorQuery(final HttpServletRequest request,
                                         final HttpServletResponse response,
                                         final UriInfo uriInfo) {
        try {
            final String[] filtersQueryStrings = uriInfo.getPath().replaceAll(".*/execute/", "").split("\\|");
            logger.info(Arrays.toString(filtersQueryStrings));
            final Executor.Context context = new Executor.Context(request, response, this.cantor);
            for (final String qs : filtersQueryStrings) {
                final String namespaceSlashFunction = qs.split(";")[0];
                final String namespace = namespaceSlashFunction.split("/")[0];
                final String functionName = namespaceSlashFunction.split("/")[1];
                final Map<String, String> params = qs.contains(";")
                        ? parseParams(qs.substring(qs.indexOf(";") + 1))
                        : Collections.emptyMap();
                logger.info("executing function '{}' with parameters: '{}'", functionName, params);
                this.functionsService.execute(namespace, functionName, context, params);
            }
            // add all headers from the result
            final Response.ResponseBuilder builder = Response.status(context.getResponseStatus() == 0 ? 200 : context.getResponseStatus());
            for (final Map.Entry<String, String> entry : context.getResponseHeaders().entrySet()) {
                builder.header(entry.getKey(), entry.getValue());
            }
            if (context.getResponseBody() != null) {
                builder.entity(context.getResponseBody());
            }
            return builder.build();
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }
    }

    private Map<String, String> parseParams(final String filterQueryString) {
        final Map<String, String> params = new HashMap<>();
        for (final String kv : filterQueryString.split(";")) {
            final String[] keyValue = kv.split("=");
            if (keyValue.length == 1) {
                continue;
            }
            params.put(keyValue[0], keyValue[1]);
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
