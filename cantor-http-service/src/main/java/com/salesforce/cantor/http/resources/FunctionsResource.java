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

    private static final String CONTEXT_KEY_PARAMS = "params";
    private static final String CONTEXT_KEY_CANTOR = "cantor";
    private static final String CONTEXT_KEY_ENTITY = "entity";

    private static final Gson parser = new Gson();

    private final Cantor cantor;
    private final FunctionsService functionsService;

    @Autowired
    public FunctionsResource(final Cantor cantor) {
        this.cantor = cantor;
        this.functionsService = new FunctionsService(cantor);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get list of all functions")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the list of all functions",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunctions() throws IOException {
        logger.info("received request for all objects namespaces");
        return Response.ok(parser.toJson(this.functionsService.getFunctionsList())).build();
    }

    @GET
    @Path("/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Provides the function with the given name",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getFunction(@PathParam("function") final String functionName) throws IOException {
        logger.info("received request for all objects namespaces");
        final String functionBody = this.functionsService.getFunction(functionName);
        // add all headers from the result
        return functionBody != null
                ? Response.ok(functionBody).header("Content-Type", MediaType.TEXT_PLAIN).build()
                : Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("/execute/{functorQuery:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute get method on functor query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the functor query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("functorQuery") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @PUT
    @Path("/execute/{functorQuery:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute put method on functor query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the functor query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response putExecuteFunction(@PathParam("functorQuery") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @POST
    @Path("/execute/{functorQuery:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute post method on functor query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the functor query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response postExecuteFunction(@PathParam("functorQuery") final String ignored,
                                        @Context final HttpServletRequest request,
                                        @Context final HttpServletResponse response,
                                        @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    @DELETE
    @Path("/execute/{functorQuery:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute delete method on functor query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Process and execute the functor query string",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response deleteExecuteFunction(@PathParam("functorQuery") final String ignored,
                                       @Context final HttpServletRequest request,
                                       @Context final HttpServletResponse response,
                                       @Context final UriInfo uriInfo) {
        return executeFunctorQuery(request, response, uriInfo);
    }

    private Response executeFunctorQuery(final HttpServletRequest request,
                                         final HttpServletResponse response,
                                         final UriInfo uriInfo) {
        try {
            final String[] filtersQueryStrings = uriInfo.getPath().replaceAll(".*/execute/", "").split("/");
            logger.info(Arrays.toString(filtersQueryStrings));
            final Entity entity = new Entity();
            for (final String qs : filtersQueryStrings) {
                final String functionName = qs.split(";")[0];
                final Map<String, String> params = qs.contains(";")
                        ? parseParams(qs.substring(qs.indexOf(";") + 1))
                        : Collections.emptyMap();
                logger.info("executing function '{}' with parameters: '{}'", functionName, params);
                final Executor.Context context = new Executor.Context(request, response, this.cantor, entity, params);
                this.functionsService.execute(functionName, context);
            }
            // add all headers from the result
            final Response.ResponseBuilder builder = Response.status(entity.getStatus() == 0 ? 200 : entity.getStatus());
            for (final Map.Entry<String, String> entry : entity.getHeadersMap().entrySet()) {
                builder.header(entry.getKey(), entry.getValue());
            }
            if (entity.getBody() != null) {
                builder.entity(entity.getBody());
            }
            return builder.build();
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }
    }

    @PUT
    @Path("/{function}")
    @Operation(summary = "Store a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Function stored"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response create(@Parameter(description = "Function identifier") @PathParam("function") final String functionName,
                           final String body) throws IOException {
        try {
            this.functionsService.storeFunction(functionName, body);
            return Response.status(Response.Status.CREATED).build();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(toString(e))
                    .build();
        }
    }

    @DELETE
    @Path("/{function}")
    @Operation(summary = "Remove a function")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Function removed"),
        @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response drop(@Parameter(description = "Namespace identifier") @PathParam("function") final String function) {
        logger.info("received request to delete function {}", function);
        return Response.ok().build();
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
