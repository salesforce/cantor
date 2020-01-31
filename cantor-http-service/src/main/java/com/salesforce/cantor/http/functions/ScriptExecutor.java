package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.script.*;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ScriptExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(ScriptExecutor.class);

    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final Cantor cantor;

    public ScriptExecutor(final Cantor cantor) {
        this.cantor = cantor;
    }

    @Override
    public List<String> getExtensions() {
        final List<String> extensions = new ArrayList<>();
        for (final ScriptEngineFactory factory : this.manager.getEngineFactories()) {
            extensions.addAll(factory.getExtensions());
        }
        return extensions;
    }

    @Override
    public Entity execute(final String functionName, final String functionBody, final Map<String, String> parameters) {
        final ScriptContext scriptContext = new SimpleScriptContext();
        // add all parameters
        for (final Map.Entry<String, String> entry : parameters.entrySet()) {
            scriptContext.setAttribute(entry.getKey(), entry.getValue(), ScriptContext.ENGINE_SCOPE);
        }
        // add cantor to parameters
        scriptContext.setAttribute("cantor", this.cantor, ScriptContext.ENGINE_SCOPE);
        final Entity entity = new Entity();
        scriptContext.setAttribute("entity", entity, ScriptContext.ENGINE_SCOPE);

        final StringWriter writer = new StringWriter();
        scriptContext.setWriter(writer);
        try {
            // run the script!
            final ScriptEngine engine = this.manager.getEngineByExtension(getExtension(functionName));
            logger.info("script engine '{}' used for function '{}'",
                    engine.getFactory().getEngineName(), functionName);
            engine.eval(functionBody, scriptContext);
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
        final String output = writer.toString();

        // if script has not set the body, set it to output
        if (entity.getBody() == null) {
            entity.setBody(output.getBytes(Charset.defaultCharset()));
        }
        return entity;
    }

    private String getExtension(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }
}

