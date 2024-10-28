package com.connectflow.dag;

import com.connectflow.dag.execution.DAGExecutionEngine;
import com.connectflow.dag.execution.DAGExecutionResult;
import com.connectflow.dag.listeners.DAGWebSocketListener;
import com.connectflow.dag.tasks.SimpleTask;
import com.connectflow.dag.tasks.Task;
import com.connectflow.dag.websocket.DAGWebSocketHandler;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import static spark.Spark.*;

public class DAGExecutionExample2 {

    public static void main(String[] args) throws InterruptedException {

        port(4567); // You can change the port if needed
        staticFiles.location("/public"); // Serve static files from src/main/resources/public
        webSocket("/dagupdates", DAGWebSocketHandler.class);
        init();

        // Create DAG execution engine with 8 threads
        DAGExecutionEngine<String, String> engine = new DAGExecutionEngine<>(8);

        // Create DAG
        DAG<String, String> dag = new DAG<>();

        // Create nodes (Nodes A to J)
        DAGNode<String, String> nodeA = new DAGNode<>("A",
                createTask("A", "Task A"), Duration.ofSeconds(620000));
        DAGNode<String, String> nodeB = new DAGNode<>("B",
                createTask("B", "Task B"), Duration.ofSeconds(3));
        DAGNode<String, String> nodeC = new DAGNode<>("C",
                createTask("C", "Task C"), Duration.ofSeconds(1));
        DAGNode<String, String> nodeD = new DAGNode<>("D",
                createTask("D", "Task D"), Duration.ofSeconds(4));
        DAGNode<String, String> nodeE = new DAGNode<>("E",
                createTask("E", "Task E"), Duration.ofSeconds(2));
        DAGNode<String, String> nodeF = new DAGNode<>("F",
                createTask("F", "Task F"), Duration.ofSeconds(3));
        DAGNode<String, String> nodeG = new DAGNode<>("G",
                createTask("G", "Task G"), Duration.ofSeconds(1));
        DAGNode<String, String> nodeH = new DAGNode<>("H",
                createTask("H", "Task H"), Duration.ofSeconds(2));
        DAGNode<String, String> nodeI = new DAGNode<>("I",
                createTask("I", "Task I"), Duration.ofSeconds(3));
        DAGNode<String, String> nodeJ = new DAGNode<>("J",
                createTask("J", "Task J"), Duration.ofSeconds(4));

        // Add nodes to DAG
        dag.addNode(nodeA);
        dag.addNode(nodeB);
        dag.addNode(nodeC);
        dag.addNode(nodeD);
        dag.addNode(nodeE);
        dag.addNode(nodeF);
        dag.addNode(nodeG);
        dag.addNode(nodeH);
        dag.addNode(nodeI);
        dag.addNode(nodeJ);

        // Add dependencies to create a complex DAG
        dag.addDependency("A", "B"); // A -> B
        dag.addDependency("A", "C"); // A -> C
        dag.addDependency("B", "D"); // B -> D
        dag.addDependency("C", "D"); // C -> D
        dag.addDependency("D", "E"); // D -> E
        dag.addDependency("D", "F"); // D -> F
        dag.addDependency("E", "G"); // E -> G
        dag.addDependency("F", "G"); // F -> G
        dag.addDependency("G", "H"); // G -> H
        dag.addDependency("H", "I"); // H -> I
        dag.addDependency("I", "J"); // I -> J

        // Add WebSocket listener
        //engine.addListener(new DAGWebSocketListener<>(dag));

        DAGWebSocketListener<String, String> dagWebSocketListener = new DAGWebSocketListener<>(dag);
        engine.addListener(dagWebSocketListener);

// Set the listener in the DAGWebSocketHandler
        DAGWebSocketHandler.setDagWebSocketListener(dagWebSocketListener);

/*
        String outputDir = "dag_visualizations"; // Ensure this directory exists or is created
        DAGVisualizationListener<String, String> visualizationListener = new DAGVisualizationListener<>(dag, outputDir);
        engine.addListener(visualizationListener);*/

        // Optionally, add the console logging listener
      /*  engine.addListener(new DAGExecutionListener<String, String>() {
            @Override
            public void onNodeStarted(DAGNode<String, String> node) {
                System.out.println("Node started: " + node.getId());
            }

            @Override
            public void onNodeCompleted(DAGNode<String, String> node, ExecutionResult<String, String> result) {
                System.out.println("Node completed: " + node.getId() + ", Result: " + result.getResult());
            }

            @Override
            public void onNodeFailed(DAGNode<String, String> node, Exception error) {
                System.out.println("Node failed: " + node.getId() + ", Error: " + error.getMessage());
            }

            @Override
            public void onDagCompleted(DAGExecutionResult<String, String> result) {
                System.out.println("DAG execution completed in " + result.getExecutionTime() + "ms");
                // Stop the server after execution is complete
                stop();
            }

            @Override
            public void updateVisualization(DAGNode<String, String> node) {
                visualizationListener.updateVisualization(node);
            }
        });*/

        try {
            // Execute DAG
            CompletableFuture<DAGExecutionResult<String, String>> future = engine.executeDag(dag);

            // Wait for completion
            DAGExecutionResult<String, String> result = future.get();

            // Print results
            System.out.println("Execution completed with status: SUCCESS");
            result.getNodeResults().forEach((id, nodeResult) -> {
                System.out.println("Node " + id + ": " + nodeResult.getStatus());
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            engine.shutdown();
            stop(); // Stop the Spark server when done
        }

        Thread.sleep(55555555);
    }

    // Create tasks for nodes
    private static Task<String, String> createTask(String id, String prefix) {
        return new SimpleTask<>(id, () -> {
            // Simulate varying task durations and potential exceptions
            if (id.equals("C")) {

               Thread.sleep(100000);
                // Introduce a failure in task F to test failure handling
               // throw new RuntimeException("Simulated failure in task F");
            }
            Thread.sleep((long) (Math.random() * 2000) + 1000);
            return prefix + " Result";
        }, Duration.ofSeconds(50000000));
    }
}
