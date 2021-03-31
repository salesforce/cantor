/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.http.jersey;

import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.servers.ServerVariable;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

@OpenAPIDefinition(
        info = @Info(
            title = "Cantor API Documentation",
            version = "1.0.0"
        ),
        servers = {
            @Server(description = "Localhost",
                    url = "http://localhost:{port}/api",
                    variables = @ServerVariable(name = "port", defaultValue = "8083")),
        }
)
class SwaggerJaxrsConfig extends ResourceConfig {
    SwaggerJaxrsConfig() {
        // cantor filters
        this.packages("com.salesforce.cantor.http.filters");

        // cantor server resources
        this.packages("com.salesforce.cantor.http.resources");

        // swagger initialization resource
        this.register(OpenApiResource.class);

        // jersey extension for file uploads
        this.register(MultiPartFeature.class);
    }
}
