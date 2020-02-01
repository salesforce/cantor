package com.salesforce.cantor.http.functions;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class FreemarkerExecutor implements Executor {
    private final Configuration configuration;

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("ftl", "freemarker");
    }

    @Override
    public void execute(final String functionName, final String functionBody, final Context context) {
        process(functionName, functionBody, context);
    }

    @Autowired
    public FreemarkerExecutor() {
        this.configuration = new Configuration(Configuration.VERSION_2_3_21);
        this.configuration.setClassForTemplateLoading(getClass(), "/");
    }

    private Entity process(final String name, final String source, final Context context) {
        try {
            final String results = doProcess(name, source, context);
            final Entity entity = context.getEntity();
            // if script has not set body, set it to the results
            if (entity.getBody() == null) {
                entity.setBody(results.getBytes(Charset.defaultCharset()));
            }
            // if script has not set the status code, set it to 200
            if (entity.getStatus() == 0) {
                entity.setStatus(200);
            }
            return entity;
        } catch (TemplateException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String doProcess(final String name,
                             final String source,
                             final Context context)
            throws IOException, TemplateException {

        final Template template = new Template(name, source, this.configuration);
        final StringWriter stringWriter = new StringWriter();

        // pass in context
        final Map<String, Object> params = new HashMap<>();
        params.put("context", context);
        // process the template
        template.process(params, stringWriter);
        return stringWriter.toString();
    }
}
