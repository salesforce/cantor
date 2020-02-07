package com.salesforce.cantor.functions;

import com.salesforce.cantor.functions.Functions.Context;
import com.salesforce.cantor.functions.Functions.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JavaExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(JavaExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Collections.singletonList("java");
    }

    @Override
    public void execute(final String function, final byte[] body, final Context context, final Map<String, String> params)
            throws IOException {
        final String functionBody = new String(body, UTF_8);
        logger.info("executing java function: {}", function);

        final Path path = saveSource(function, functionBody);
        try {
            final Object instance = runClass(function, compileSource(function, path));
            final String methodName = params.get(".method");
            final Method method = instance.getClass().getMethod(methodName, Context.class, Map.class);
            method.invoke(instance, context, params);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Path compileSource(final String name, final Path javaFile) {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final int results = compiler.run(System.in, System.out, outputStream, javaFile.toFile().getAbsolutePath());
        if (results != 0) {
            throw new RuntimeException(new String(outputStream.toByteArray()));
        }
        return javaFile.getParent().resolve(name);
    }

    private Object runClass(final String name, final Path javaClass)
            throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        URL classUrl = javaClass.getParent().toFile().toURI().toURL();
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classUrl});
        Class<?> clazz = Class.forName(name.substring(0, name.indexOf(".java")), true, classLoader);
        return clazz.newInstance();
    }


    private Path saveSource(final String name, final String source) throws IOException {
        String tmpProperty = System.getProperty("java.io.tmpdir");
        Path sourcePath = Paths.get(tmpProperty, name);
        Files.write(sourcePath, source.getBytes(UTF_8));
        return sourcePath;
    }
}
