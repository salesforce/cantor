package com.salesforce.cantor.functions;

import com.salesforce.cantor.functions.Functions.Context;
import com.salesforce.cantor.functions.Functions.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(ScriptExecutor.class);

    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final Map<String, ScriptEngine> scriptEngines = new HashMap<>();

    @Override
    public List<String> getExtensions() {
        final List<String> extensions = new ArrayList<>();
        for (final ScriptEngineFactory factory : this.manager.getEngineFactories()) {
            extensions.addAll(factory.getExtensions());
        }
        return extensions;
    }

    @Override
    public void execute(final String function,
                        final byte[] body,
                        final Context context, Map<String, String> params) {
        final String scriptBody = new String(body, StandardCharsets.UTF_8);
        final ScriptContext scriptContext = new SimpleScriptContext();
        // add all parameters
        scriptContext.setAttribute("context", context, ScriptContext.ENGINE_SCOPE);
        scriptContext.setAttribute("params", params, ScriptContext.ENGINE_SCOPE);

        final StringWriter writer = new StringWriter();
        scriptContext.setWriter(writer);
        try {
            // run the script!
            final ScriptEngine engine = getEngine(getExtension(function));
            logger.info("script engine '{}' used for function '{}'", engine.getFactory().getEngineName(), function);
            engine.eval(scriptBody, scriptContext);
            if (params.get(".method") != null) {
                final String methodName = params.get(".method");
                final Invocable invocableEngine = (Invocable) engine;
                invocableEngine.invokeFunction(methodName);
            }
        } catch (ScriptException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    // cache loaded script engines
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

