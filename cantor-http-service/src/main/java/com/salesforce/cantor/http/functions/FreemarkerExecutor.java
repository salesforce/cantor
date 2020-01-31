package com.salesforce.cantor.http.functions;

import com.salesforce.cantor.Cantor;
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

    private final Cantor cantor;

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("ftl", "freemarker");
    }

    @Override
    public Entity execute(final String functionName, final String functionBody, final Map<String, String> parameters) {
        return process(functionName, functionBody, parameters);
    }

    @Autowired
    public FreemarkerExecutor(final Cantor cantor) {
        this.cantor = cantor;
        this.configuration = new Configuration(Configuration.VERSION_2_3_21);
        this.configuration.setClassForTemplateLoading(getClass(), "/");
    }

    private Entity process(final String name, final String source, final Map<String, String> parameters) {
        try {
            final Entity entity = new Entity();
            final String results = doProcess(name, source, entity, parameters);
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
                             final Entity entity,
                             final Map<String, String> parameters)
            throws IOException, TemplateException {

        final Template template = new Template(name, source, this.configuration);
        final StringWriter stringWriter = new StringWriter();

        // copy over parameters and add 'cantor' to it
        final Map<String, Object> params = new HashMap<>(parameters);
        params.put("cantor", this.cantor);
        params.put("entity", entity);

        // process the template
        template.process(params, stringWriter);
        return stringWriter.toString();
    }
}
