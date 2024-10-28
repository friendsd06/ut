package com.connectflow.dag.execution;

import com.connectflow.dag.*;
import com.connectflow.dag.listeners.DAGExecutionListener;
import com.connectflow.dag.metrics.DAGProgressTracker;
import com.connectflow.dag.planner.DAGExecutionPlan;
import com.connectflow.dag.planner.DAGExecutionPlanner;
import com.connectflow.dag.tasks.Task;
import com.connectflow.dag.tasks.TaskStatus;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DAGExecutionEngine<T, R> {

    private final ExecutorService executorService;
    private final DAGExecutionPlanner<T, R> executionPlanner;
    private final DAGProgressTracker<T, R> progressTracker;
    private final Set<DAGExecutionListener<T, R>> listeners;
    private final Map<T, CompletableFuture<ExecutionResult<T, R>>> futures;
    private final AtomicInteger activeTaskCount;

    public DAGExecutionEngine(int threads) {
        this.executorService = Executors.newFixedThreadPool(threads);
        this.executionPlanner = new DAGExecutionPlanner<>();
        this.progressTracker = new DAGProgressTracker<>();
        this.listeners = ConcurrentHashMap.newKeySet();
        this.futures = new ConcurrentHashMap<>();
        this.activeTaskCount = new AtomicInteger(0);
    }

    public void addListener(DAGExecutionListener<T, R> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }
        listeners.add(listener);
    }

    public CompletableFuture<DAGExecutionResult<T, R>> executeDag(DAG<T, R> dag) {
        dag.validateDAG();
        DAGExecutionPlan<T, R> plan = executionPlanner.createExecutionPlan(dag);
        return executeAccordingToPlan(plan);
    }

    private CompletableFuture<DAGExecutionResult<T, R>> executeAccordingToPlan(
            DAGExecutionPlan<T, R> plan) {
        CompletableFuture<DAGExecutionResult<T, R>> dagFuture = new CompletableFuture<>();

        Set<DAGNode<T, R>> initialNodes = plan.getInitialNodes();
        executeNodes(initialNodes, plan, dagFuture);

        return dagFuture;
    }

    private void executeNodes(Set<DAGNode<T, R>> nodes,
                              DAGExecutionPlan<T, R> plan,
                              CompletableFuture<DAGExecutionResult<T, R>> dagFuture) {
        for (DAGNode<T, R> node : nodes) {
            if (canExecuteNode(node)) {
                executeNode(node, plan, dagFuture);
            }
        }
    }

    private void executeNode(DAGNode<T, R> node,
                             DAGExecutionPlan<T, R> plan,
                             CompletableFuture<DAGExecutionResult<T, R>> dagFuture) {
        CompletableFuture<ExecutionResult<T, R>> future = new CompletableFuture<>();
        futures.put(node.getId(), future);

        executorService.submit(() -> {
            try {
                activeTaskCount.incrementAndGet();
                node.setStatus(TaskStatus.RUNNING);
                notifyNodeStarted(node);

                ExecutionResult<T, R> result = executeTask(node.getTask());
                handleNodeCompletion(node, result, plan, dagFuture);

            } catch (Exception e) {
                handleNodeFailure(node, e, plan, dagFuture);
            } finally {
                activeTaskCount.decrementAndGet();
            }
        });
    }

    private ExecutionResult<T, R> executeTask(Task<T, R> task) throws Exception {
        long startTime = System.currentTimeMillis();

        Future<R> execution = executorService.submit(task::execute);
        R result = execution.get(task.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

        return new ExecutionResult.Builder<T, R>(task.getId())
                .status(TaskStatus.COMPLETED)
                .result(result)
                .startTime(startTime)
                .endTime(System.currentTimeMillis())
                .build();
    }

    private void handleNodeCompletion(DAGNode<T, R> node,
                                      ExecutionResult<T, R> result,
                                      DAGExecutionPlan<T, R> plan,
                                      CompletableFuture<DAGExecutionResult<T, R>> dagFuture) {
        node.setResult(result);
        node.setStatus(TaskStatus.COMPLETED);
        plan.markNodeCompleted(node);
        notifyNodeCompleted(node, result);

        CompletableFuture<ExecutionResult<T, R>> future = futures.get(node.getId());
        if (future != null) {
            future.complete(result);
        }

        // Update progress
        progressTracker.updateProgress(node, result);

        // Check if DAG execution is complete
        if (plan.isExecutionComplete()) {
            completeDagExecution(plan, dagFuture);
        } else {
            // Schedule ready successor nodes
            Set<DAGNode<T, R>> readyNodes = plan.getReadyNodes();
            executeNodes(readyNodes, plan, dagFuture);
        }
    }

    private void handleNodeFailure(DAGNode<T, R> node,
                                   Exception error,
                                   DAGExecutionPlan<T, R> plan,
                                   CompletableFuture<DAGExecutionResult<T, R>> dagFuture) {
        node.setStatus(TaskStatus.FAILED);
        notifyNodeFailed(node, error);

        CompletableFuture<ExecutionResult<T, R>> future = futures.get(node.getId());
        if (future != null) {
            future.completeExceptionally(error);
        }

        // Handle failure based on failure strategy
        FailureStrategy strategy = plan.getFailureStrategy();
        switch (strategy) {
            case FAIL_FAST:
                dagFuture.completeExceptionally(error);
                break;
            case CONTINUE:
                // Continue with other independent paths
                Set<DAGNode<T, R>> readyNodes = plan.getReadyNodes();
                executeNodes(readyNodes, plan, dagFuture);
                break;
            case RETRY:
                // Implement retry logic if needed
                dagFuture.completeExceptionally(error);
                break;
        }
    }

    private boolean canExecuteNode(DAGNode<T, R> node) {
        return node.getIncomingNodes().stream()
                .allMatch(parent -> parent.getStatus() == TaskStatus.COMPLETED);
    }

    private void completeDagExecution(DAGExecutionPlan<T, R> plan,
                                      CompletableFuture<DAGExecutionResult<T, R>> dagFuture) {
        DAGExecutionResult<T, R> result = createDagResult(plan);
        notifyDagCompleted(result);
        dagFuture.complete(result);
    }

    // Listener notifications
    private void notifyNodeStarted(DAGNode<T, R> node) {
        listeners.forEach(listener -> {
            try {
                listener.onNodeStarted(node);
            } catch (Exception e) {
                // Log listener exception
            }
        });
    }

    private void notifyNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {
        listeners.forEach(listener -> {
            try {
                listener.onNodeCompleted(node, result);
            } catch (Exception e) {
            }
        });
    }

    private void notifyNodeFailed(DAGNode<T, R> node, Exception error) {
        listeners.forEach(listener -> {
            try {
                listener.onNodeFailed(node, error);
            } catch (Exception e) {
            }
        });
    }

    private void notifyDagCompleted(DAGExecutionResult<T, R> result) {
        listeners.forEach(listener -> {
            try {
                listener.onDagCompleted(result);
            } catch (Exception e) {
            }
        });
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private DAGExecutionResult<T, R> createDagResult(DAGExecutionPlan<T, R> plan) {
        return new DAGExecutionResult.Builder<T, R>()
                .setStartTime(plan.getStartTime())
                .setEndTime(System.currentTimeMillis())
                .setNodeResults(collectNodeResults(plan))
                .setExecutionMetrics(progressTracker.getMetrics())
                .build();
    }

    private Map<T, ExecutionResult<T, R>> collectNodeResults(DAGExecutionPlan<T, R> plan) {
        return plan.getAllNodes().stream()
                .collect(Collectors.toMap(
                        DAGNode::getId,
                        node -> Optional.ofNullable(node.getResult())
                                .orElse(createEmptyResult(node))
                ));
    }

    private ExecutionResult<T, R> createEmptyResult(DAGNode<T, R> node) {
        return new ExecutionResult.Builder<T, R>(node.getId())
                .status(TaskStatus.SKIPPED)
                .startTime(0)
                .endTime(0)
                .build();
    }
}
