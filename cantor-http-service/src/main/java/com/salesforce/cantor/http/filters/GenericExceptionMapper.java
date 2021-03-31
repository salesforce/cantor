package com.salesforce.cantor.http.filters;

import com.google.gson.Gson;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);
    private static final Gson parser = new Gson();

    @Context
    UriInfo uriInfo;

    @Override
    public Response toResponse(final Throwable throwable) {
        logger.warn("exception for url: {}", this.uriInfo.getPath());
        logger.error("caught an exception: ", throwable);

        if (throwable instanceof IllegalArgumentException
                || throwable instanceof IOException
                || throwable instanceof BadRequestException) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .entity(toJson(throwable))
                    .build();
        }

        if (throwable instanceof NotFoundException) {
            return Response.status(Response.Status.NOT_FOUND)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .entity(toJson(throwable))
                    .build();
        }

        if (throwable instanceof AuthenticationException) {
            return Response.status(Response.Status.FORBIDDEN)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .entity(toJson(throwable))
                    .build();
        }

        if (throwable instanceof NotAuthorizedException) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .header("Content-Type", MediaType.APPLICATION_JSON)
                    .entity(toJson(throwable))
                    .build();
        }


        return Response.serverError()
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .entity(toJson(throwable))
                .build();
    }

    private String toJson(final Throwable throwable) {
        final Map<String, String> exception = new HashMap<>();
        exception.put("message", throwable.getMessage());
        exception.put("stack-trace", ExceptionUtils.getStackTrace(throwable));
        return parser.toJson(exception);
    }
}
