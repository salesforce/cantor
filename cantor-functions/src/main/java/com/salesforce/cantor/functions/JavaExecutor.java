package com.salesforce.cantor.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JavaExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(JavaExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Collections.singletonList("java");
    }

    @Override
    public void execute(final String function,
                        final byte[] body,
                        final Context context,
                        final Map<String, String> params)
            throws IOException {
        final String tempDirectory = UUID.randomUUID().toString();
        final String javaSource = new String(body, UTF_8);
        final Path path = saveSource(tempDirectory, function, javaSource);
        try {
            final String className = params.get(".class");
            final String methodName = params.get(".method");
            logger.info("executing java function: {} class: {} method: {}", function, className, methodName);
            compileSource(function, path);
            logger.info("class path is: {}", path);
            final Object instance = getClassInstance(className, path);
            final Method method = instance.getClass().getMethod(methodName, Context.class, Map.class);
            method.invoke(instance, context, params);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        } finally {
            deleteSource(tempDirectory);
        }
    }

    private void compileSource(final String name, final Path javaFile) {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final int results = compiler.run(System.in, System.out, outputStream, javaFile.toFile().getAbsolutePath());
        if (results != 0) {
            throw new RuntimeException(new String(outputStream.toByteArray()));
        }
    }

    private Object getClassInstance(final String className, final Path javaClass)
            throws ReflectiveOperationException, MalformedURLException {
        final URL classUrl = javaClass.getParent().toFile().toURI().toURL();
        final URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classUrl});
        return Class.forName(className, true, classLoader).getDeclaredConstructor().newInstance();
    }

    private Path saveSource(final String namespace, final String name, final String source) throws IOException {
        final String tmpProperty = System.getProperty("java.io.tmpdir");
        Files.createDirectory(Paths.get(tmpProperty, namespace));
        final Path sourcePath = Paths.get(tmpProperty, namespace, name);
        Files.write(sourcePath, source.getBytes(UTF_8));
        return sourcePath;
    }

    private void deleteSource(final String tempDirectory) throws IOException {
        final String tmpProperty = System.getProperty("java.io.tmpdir");
        Files.delete(Paths.get(tmpProperty, tempDirectory));
    }

    private Path getBaseDirectory(final String namespace) throws IOException {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        return Files.createDirectory(Paths.get(tmpDir, namespace));
    }
}
