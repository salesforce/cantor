/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.functions.executors;

import com.salesforce.cantor.functions.Context;
import com.salesforce.cantor.functions.Executor;
import com.salesforce.cantor.functions.Functions;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This executor processes an FTL template with the given parameters, and stores the generated result
 * in the special context variable '.out'.
 *
 * Two special parameters are injected into the FTL engine:
 *   - 'context': the Context object for the function
 *   - 'params': the map of parameters pass to executor
 */
public class FreemarkerExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(FreemarkerExecutor.class);
    private Configuration configuration = null;

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("ftl", "freemarker");
    }

    @Override
    public void run(final String function,
                    final byte[] body,
                    final Context context,
                    final Map<String, String> params) {
        init(context.getFunctions());
        final String templateBody = new String(body, StandardCharsets.UTF_8);
        process(function, templateBody, context, params);
    }

    private void init(final Functions functions) {
        if (this.configuration != null) {
            // already initialized
            return;
        }

        this.configuration = new Configuration(Configuration.VERSION_2_3_21);
        this.configuration.setTemplateLoader(new TemplateLoader() {
            @Override
            public Object findTemplateSource(final String name) throws IOException {
                logger.info("finding ftl template source for name: {}", name);
                // use the function name as template source name if found; null otherwise
                final byte[] functionBody = functions.get(name);
                return functionBody == null ? null : name;
            }

            @Override
            public long getLastModified(final Object name) {
                // this requires a call to the database; we might as well just fetch the function body always
                return System.currentTimeMillis();
            }

            @Override
            public Reader getReader(final Object name, final String encoding) throws IOException {
                final byte[] functionBody = functions.get((String) name);
                return functionBody == null ? null : new StringReader(new String(functionBody, encoding));
            }

            @Override
            public void closeTemplateSource(Object ignored) {
                // no-op
            }
        });
    }

    private void process(final String name, final String source, final Context context, final Map<String, String> params) {
        try {
            final String results = doProcess(name, source, context, params);
            // if script has not set body, set it to the results
            if (context.get("http.entity") == null) {
                context.set("http.entity", results);
            }
        } catch (TemplateException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String doProcess(final String name,
                             final String source,
                             final Context context,
                             final Map<String, String> params)
            throws IOException, TemplateException {

        final Template template = new Template(name, source, this.configuration);
        final StringWriter stringWriter = new StringWriter();

        // pass in context
        final Map<String, Object> model = new HashMap<>();
        model.put("context", context);
        model.put("params", params);
        // process the template
        template.process(model, stringWriter);
        return stringWriter.toString();
    }
}
