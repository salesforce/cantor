package com.salesforce.cantor.misc.auth;

import com.salesforce.cantor.*;
import com.salesforce.cantor.grpc.auth.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class AuthorizedCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizedCantor.class);

    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public AuthorizedCantor(final Cantor delegate,
                            final Function<AbstractBaseAuthorizedNamespaceable.Request, Boolean> requestFunction,
                            final String adminPassword) throws IOException {
        checkArgument(delegate != null, "null delegate");

        logger.info("new instance of authorized cantor created");

        this.objects = new AuthorizedObjects(delegate.objects(), requestFunction);
        this.sets = new AuthorizedSets(delegate.sets(), requestFunction);
        this.events = new AuthorizedEvents(delegate.events(), requestFunction);

        AuthUtils.initializeRoles(delegate.objects());
        AuthUtils.initializeAdmin(delegate.objects(), adminPassword);
    }

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        return this.sets;
    }

    @Override
    public Events events() {
        return this.events;
    }
}
