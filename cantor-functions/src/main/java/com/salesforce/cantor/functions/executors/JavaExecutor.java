package com.salesforce.cantor.functions.executors;

import com.salesforce.cantor.functions.Context;
import com.salesforce.cantor.functions.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.File;
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

/**
 * Executor for functions implemented in Java. The executor expects two special parameters:
 * '.class' and '.method' to indicate the class name and the name of the method to invoke.
 */
// TODO instead of storing the source and compiling on each execution, store the compiled class object in cantor
public class JavaExecutor implements Executor {
    private static final Logger logger = LoggerFactory.getLogger(JavaExecutor.class);

    @Override
    public List<String> getExtensions() {
        return Collections.singletonList("java");
    }

    @Override
    public void run(final String function,
                    final byte[] body,
                    final Context context,
                    final Map<String, String> params)
            throws IOException {

        // two special parameters have to exist: .class and .method to indicate the class and method name
        final String className = params.get(".class");
        if (className == null) {
            throw new RuntimeException("missing '.class' parameter");
        }
        final String methodName = params.get(".method");
        if (methodName == null) {
            throw new RuntimeException("missing '.method' parameter");
        }

        final Path tempPath = getBaseDirectory(UUID.randomUUID().toString());
        // create a temp directory, store the source code, compile, and invoke the given class.method
        try {
            logger.info("executing java function: {} class: {} method: {}", function, className, methodName);
            final String javaSource = new String(body, UTF_8);
            final Path path = saveSource(tempPath, function, javaSource);
            compileSource(path);
            logger.info("class path is: {}", path);
            final Object instance = getClassInstance(className, path);
            final Method method = instance.getClass().getMethod(methodName, Context.class, Map.class);
            if (method == null) {
                throw new RuntimeException("method with signature " + className + "." + methodName + "(Context, Map) not found");
            }
            method.invoke(instance, context, params);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        } finally {
            deleteDirectory(tempPath.toFile());
        }
    }

    private void compileSource(final Path javaFile) {
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

    private Path saveSource(final Path path, final String name, final String source) throws IOException {
        logger.info("storing function: {} at: {}", name, path);
        final Path sourcePath = path.resolve(name);
        Files.write(sourcePath, source.getBytes(UTF_8));
        return sourcePath;
    }

    private void deleteDirectory(final File file) throws IOException {
        if (file.isDirectory()) {
            final File[] list = file.listFiles();
            if (list == null) {
                return;
            }
            for (final File f : list) {
                if (f != null && f.isDirectory()) {
                    deleteDirectory(f);
                }
            }
        }
        if (!file.delete()) {
            throw new IOException("failed to delete: " + file.getAbsolutePath());
        }
    }

    private Path getBaseDirectory(final String namespace) throws IOException {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        return Files.createDirectory(Paths.get(tmpDir, namespace));
    }
}
