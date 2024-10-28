package com.connectflow.dag.tasks;
import java.time.Duration;
import java.util.concurrent.Callable;

public class SimpleTask<T, R> implements Task<T, R> {
    private final T id;
    private final Callable<R> action;
    private final Duration timeout;

    public SimpleTask(T id, Callable<R> action, Duration timeout) {
        if (id == null || action == null || timeout == null) {
            throw new IllegalArgumentException("id, action, and timeout must not be null");
        }
        this.id = id;
        this.action = action;
        this.timeout = timeout;
    }

    @Override
    public T getId() {
        return id;
    }

    @Override
    public Duration getTimeout() {
        return timeout;
    }

    @Override
    public R execute() throws Exception {
        return action.call();
    }
}
