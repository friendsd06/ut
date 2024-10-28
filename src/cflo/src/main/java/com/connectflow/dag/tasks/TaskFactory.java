package com.connectflow.dag.tasks;

public interface TaskFactory<T, R> {
    Task<T, R> createTask(T id);
}

