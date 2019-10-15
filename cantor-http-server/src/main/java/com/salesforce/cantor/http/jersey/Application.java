package com.salesforce.cantor.http.jersey;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedHttpServer.class);

    private static final String basePath = "/api/*";
    private static final int port = 8083;

    public static void main(String[] args) {
        logger.info("starting cantor http server...");
        final Server server = new EmbeddedHttpServer().createServer(port, basePath);

        try {
            server.start();
            System.out.println("Server started: http://localhost:" + port);
            server.join();
        } catch (final Exception e) {
            logger.error("failed to start Cantor HTTP Server: ", e);
            System.exit(1);
        } finally {
            server.destroy();
        }
    }
}
