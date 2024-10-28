package com.connectflow.dag.tasks;

import java.time.Duration;

public interface Task<T, R> {
    T getId();

    Duration getTimeout();

    R execute() throws Exception;
}
