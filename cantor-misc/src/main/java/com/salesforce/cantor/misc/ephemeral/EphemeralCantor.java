package com.salesforce.cantor.misc.ephemeral;

import com.salesforce.cantor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * This class expects a {@link Function} which will be called everytime and instance from Cantor is requested.
 * This will allow dynamic the creation of Cantor for instances that may need dynamic parameters.
 *
 * Example:
 * <pre>
 *  final EphemeralCantor cantor = new EphemeralCantor<String>((path) -> { new CantorOnH2(path) });
 *  ...
 *  final UserRequest request = ...
 *  final String path = request.getParam("path");
 *  cantor.setFunctionParam(path);
 *  cantor.objects().namespaces(); // initializes h2 cantor with path provided by user
 * </pre>
 */
public class EphemeralCantor<T> implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(EphemeralCantor.class);
    private final Function<T, Cantor> cantorGenerator;
    private T functionParam;

    public EphemeralCantor(final Function<T, Cantor> cantorGenerator) {
        this.cantorGenerator = cantorGenerator;
        logger.info("new instance of ephemeral cantor created");
    }

    @Override
    public Objects objects() {
        try {
            return this.cantorGenerator.apply(this.functionParam).objects();
        } catch (final Exception e) {
            throw new RuntimeException("Cantor failed to created.", e);
        }
    }

    @Override
    public Sets sets() {
        try {
            return this.cantorGenerator.apply(this.functionParam).sets();
        } catch (final Exception e) {
            throw new RuntimeException("Cantor failed to created.", e);
        }

    }

    @Override
    public Events events() {
        try {
            return this.cantorGenerator.apply(this.functionParam).events();
        } catch (final Exception e) {
            throw new RuntimeException("Cantor failed to created.", e);
        }

    }

    /**
     * Call this to set the field that will be passed to the function
     * @param functionParam passed to the cantor initialization function
     */
    public void setFunctionParam(final T functionParam) {
        this.functionParam = functionParam;
    }

    public T getFunctionParam() {
        return this.functionParam;
    }
}
