package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.script.*;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ScriptExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(ScriptExecutor.class);

    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final Map<String, ScriptEngine> scriptEngines = new HashMap<>();
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
        Entity entity = new Entity();
        scriptContext.setAttribute("entity", entity, ScriptContext.ENGINE_SCOPE);

        final StringWriter writer = new StringWriter();
        scriptContext.setWriter(writer);
        try {
            final long start = System.nanoTime();
            // run the script!
            final ScriptEngine engine = getEngine(getExtension(functionName));
            final long middle = System.nanoTime();
            logger.info("script engine '{}' used for function '{}'",
                    engine.getFactory().getEngineName(), functionName);
            engine.eval(functionBody, scriptContext);
            final long end = System.nanoTime();
            logger.info("{} {}", middle - start, end - middle);
            if (parameters.containsKey(".method")) {
                final String methodName = parameters.get(".method");
                final Invocable invocableEngine = (Invocable) engine;
                invocableEngine.invokeFunction(methodName);
            }
        } catch (ScriptException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        // if script has not set the body, set it to output
        if (entity.getBody() == null || entity.getBody().length == 0) {
            final String output = writer.toString();
            entity.setBody(output.getBytes(Charset.defaultCharset()));
            entity.setStatus(200);
        }
        return entity;
    }

    private synchronized ScriptEngine getEngine(final String extension) {
        if (this.scriptEngines.containsKey(extension)) {
            return this.scriptEngines.get(extension);
        }
        final ScriptEngine engine = this.manager.getEngineByExtension(extension);
        this.scriptEngines.put(extension, engine);
        return engine;
    }

    private String getExtension(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }
}

