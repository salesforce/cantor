package com.salesforce.cantor.http.jersey;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.h2.H2DataSourceProperties;
import com.salesforce.cantor.h2.H2DataSourceProvider;
import com.salesforce.cantor.misc.loggable.LoggableCantor;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EmbeddedHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

    Server createServer(final int port, final String basePath) {
        final ResourceConfig config = new SwaggerJaxrsConfig();

        // bind resources with required constructor parameters
        final Cantor cantor = getCantor();
        config.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new EventsResource(cantor));
                bind(new ObjectsResource(cantor));
                bind(new SetsResource(cantor));
                bind(new MapsResource(cantor));
            }
        });

        final Server server = new Server(port);

        // load jersey servlets
        final ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(config));
        final ServletContextHandler context = new ServletContextHandler(server, "/");
        context.addServlet(jerseyServlet, basePath);

        // serve static resources
        context.setResourceBase("cantor-http-server/src/main/resources/static");
        context.addServlet(DefaultServlet.class, "/");

        return server;
    }

    private Cantor getCantor() {
        try {
            // set up a simple cantor using H2
            final H2DataSourceProperties h2Properties = new H2DataSourceProperties()
                    .setPath("/tmp/cantor-server-tmp/")
                    .setInMemory(false)
                    .setCompressed(false)
                    .setUsername("cantor")
                    .setPassword("");

            final CantorOnH2 cantorOnH2 = new CantorOnH2(H2DataSourceProvider.getDatasource(h2Properties));
            // use loggable wrapper so all calls to cantor will be logged
            return new LoggableCantor(cantorOnH2);
        } catch (final IOException e) {
            logger.error("failed to initialize cantor:", e);
        }
        return null;
    }
}
