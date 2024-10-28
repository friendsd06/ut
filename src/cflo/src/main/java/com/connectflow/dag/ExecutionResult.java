package com.connectflow.dag;

import com.connectflow.dag.tasks.TaskStatus;

public class ExecutionResult<T, R> {
    private final T taskId;
    private final TaskStatus status;
    private final R result;
    private final long startTime;
    private final long endTime;

    private ExecutionResult(Builder<T, R> builder) {
        this.taskId = builder.taskId;
        this.status = builder.status;
        this.result = builder.result;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
    }

    public T getTaskId() {
        return taskId;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public R getResult() {
        return result;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getDuration() {
        return endTime - startTime;
    }

    public static class Builder<T, R> {
        private final T taskId;
        private TaskStatus status;
        private R result;
        private long startTime;
        private long endTime;

        public Builder(T taskId) {
            if (taskId == null) {
                throw new IllegalArgumentException("Task ID cannot be null");
            }
            this.taskId = taskId;
        }

        public Builder<T, R> status(TaskStatus status) {
            this.status = status;
            return this;
        }

        public Builder<T, R> result(R result) {
            this.result = result;
            return this;
        }

        public Builder<T, R> startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder<T, R> endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public ExecutionResult<T, R> build() {
            return new ExecutionResult<>(this);
        }
    }
}
