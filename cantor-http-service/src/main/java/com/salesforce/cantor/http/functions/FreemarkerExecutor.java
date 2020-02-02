package com.salesforce.cantor.http.functions;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringWriter;
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
    public void execute(final String functionName, final String functionBody, final Context context, Map<String, String> params) {
        process(functionName, functionBody, context);
    }

    @Autowired
    public FreemarkerExecutor() {
        this.configuration = new Configuration(Configuration.VERSION_2_3_21);
        this.configuration.setClassForTemplateLoading(getClass(), "/");
    }

    private void process(final String name, final String source, final Context context) {
        try {
            final String results = doProcess(name, source, context);
            // if script has not set body, set it to the results
            if (context.getResponseBody() == null) {
                context.setResponseBody(results);
            }
            // if script has not set the status code, set it to 200
            if (context.getResponseStatus() == 0) {
                context.setResponseStatus(200);
            }
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
