package com.salesforce.cantor.http.jersey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

    @Override
    public Response toResponse(final Throwable throwable) {

        if (throwable instanceof IllegalArgumentException) {
            logger.warn("bad request exception: {}", throwable.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        if (throwable instanceof NotFoundException) {
            logger.warn("not found exception: {}", throwable.getMessage());
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        logger.error("caught an exception: ", throwable);
        return Response.serverError().build();
    }
}
