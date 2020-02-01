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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Path("/functions")
@Tag(name = "Functions Resource", description = "Api for handling Cantor functions")
public class FunctionsResource {
    private static final Logger logger = LoggerFactory.getLogger(FunctionsResource.class);
    private static final String serverErrorMessage = "Internal server error occurred";

    private static final Gson parser = new Gson();

    private final FunctionsService functionsService;

    @Autowired
    public FunctionsResource(final Cantor cantor) {
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
    @Path("/execute/{function}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Execute a function")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Execute the function with the given name",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "500", description = serverErrorMessage)
    })
    public Response getExecuteFunction(@PathParam("function") final String functionName,
                                       @Context final UriInfo uriInfo) throws IOException {
        final Entity result;
        try {
            final Map<String, String> parameters = new HashMap<>();
            for (final Map.Entry<String, List<String>> entry : uriInfo.getQueryParameters().entrySet()) {
                parameters.put(entry.getKey(), entry.getValue().get(0));
            }
            logger.info("executing function '{}' with parameters: '{}'", functionName, parameters);
            result = this.functionsService.execute(functionName, parameters);
        } catch (Exception e) {
            return Response.serverError()
                    .header("Content-Type", "text/plain")
                    .entity(toString(e))
                    .build();
        }

        // add all headers from the result
        final Response.ResponseBuilder builder = Response.status(result.getStatus() == 0 ? 200 : result.getStatus());
        for (final Map.Entry<String, String> entry : result.getHeadersMap().entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
        if (result.getBody() != null && result.getBody().length > 0) {
            builder.entity(result.getBody());
        }
        return builder.build();
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
    public Response drop(@Parameter(description = "Namespace identifier") @PathParam("function") final String function) throws IOException {
        logger.info("received request to delete function {}", function);
        return Response.ok().build();
    }

    private String toString(final Throwable throwable) {
        final StringWriter writer = new StringWriter();
        final PrintWriter printer = new PrintWriter(writer);
        throwable.printStackTrace(printer);
        return writer.toString();
    }
}
