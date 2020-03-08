/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.functions.executors;

import com.salesforce.cantor.functions.Context;
import com.salesforce.cantor.functions.Executor;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.IOException;
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
    private final Configuration configuration;

    @Override
    public List<String> getExtensions() {
        return Arrays.asList("ftl", "freemarker");
    }

    @Override
    public void run(final String function,
                    final byte[] body,
                    final Context context,
                    final Map<String, String> params) {
        final String templateBody = new String(body, StandardCharsets.UTF_8);
        process(function, templateBody, context, params);
    }

    public FreemarkerExecutor() {
        this.configuration = new Configuration(Configuration.VERSION_2_3_21);
        this.configuration.setClassForTemplateLoading(getClass(), "/");
    }

    private void process(final String name, final String source, final Context context, final Map<String, String> params) {
        try {
            final String results = doProcess(name, source, context, params);
            // if script has not set body, set it to the results
            if (context.get(".out") == null) {
                context.set(".out", results);
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
