/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.jersey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

    @Context UriInfo uriInfo;

    @Override
    public Response toResponse(final Throwable throwable) {

        if (throwable instanceof IllegalArgumentException) {
            logger.warn("bad request exception: ", throwable);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        if (throwable instanceof NotFoundException) {
            logger.warn("not found exception: ", throwable);
            logger.warn("url info: {}", this.uriInfo.getPath());
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        logger.error("caught an exception: ", throwable);
        return Response.serverError().build();
    }
}
