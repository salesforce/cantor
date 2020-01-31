package com.salesforce.cantor.http.functions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExecutorsService {

    private final List<Executor> executors;

    @Autowired
    public ExecutorsService(final List<Executor> executors) {
        if (executors == null || executors.isEmpty()) {
            throw new IllegalStateException("no executor found");
        }
        this.executors = executors;
    }

    public List<Executor> getExecutors() {
        return this.executors;
    }

    // return the executor instance for the given executor name
    public Executor getExecutor(final String functionName) {
        final String extension = getExtension(functionName);
        for (final Executor executor : this.executors) {
            if (executor.getExtensions().contains(extension)) {
                return executor;
            }
        }
        throw new IllegalArgumentException("executor for extension '" + extension + "' not found");
    }

    private String getExtension(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }
}
