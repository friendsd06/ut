package com.connectflow.dag.listeners;

import com.connectflow.dag.DAG;
import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;
import com.connectflow.dag.tasks.TaskStatus;
import com.connectflow.dag.websocket.DAGWebSocketHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.eclipse.jetty.websocket.api.Session;
import java.util.*;

public class DAGWebSocketListener<T, R> implements DAGExecutionListener<T, R> {

    private final DAG<T, R> dag;
    private final Gson gson ;
    private String initialGraphMessage;

    public DAGWebSocketListener(DAG<T, R> dag) {
        this.dag = dag;
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
        this.initialGraphMessage = createInitialGraphMessage();
    }

    // Changed method signature to create the initial graph message
    private String createInitialGraphMessage() {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "graph");
        message.put("elements", buildGraphElements());
        String jsonMessage = gson.toJson(message);
        System.out.println("Created initial graph message: " + jsonMessage);
        return jsonMessage;
    }

    // Send the initial graph to a specific session
    public void sendInitialGraph(Session session) {
        System.out.println("Sending initial graph to session: " + session);
        DAGWebSocketHandler.sendMessage(session, initialGraphMessage);
    }

    private List<Map<String, Object>> buildGraphElements() {
        List<Map<String, Object>> elements = new ArrayList<>();

        // Add nodes
        for (DAGNode<T, R> node : dag.getNodes()) {
            Map<String, Object> nodeData = new HashMap<>();
            Map<String, Object> data = new HashMap<>();
            data.put("id", node.getId().toString());
            nodeData.put("data", data);
            elements.add(nodeData);
        }

        // Add edges
        for (DAGNode<T, R> node : dag.getNodes()) {
            for (DAGNode<T, R> targetNode : node.getOutgoingNodes()) {
                Map<String, Object> edgeData = new HashMap<>();
                Map<String, Object> data = new HashMap<>();
                data.put("id", node.getId() + "->" + targetNode.getId());
                data.put("source", node.getId().toString());
                data.put("target", targetNode.getId().toString());
                edgeData.put("data", data);
                elements.add(edgeData);
            }
        }

        return elements;
    }

    private void sendStatusUpdate(DAGNode<T, R> node, TaskStatus status) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "status");
        message.put("nodeId", node.getId().toString());
        message.put("status", status.name());
        String jsonMessage = gson.toJson(message);
        System.out.println("Sending status update: " + jsonMessage);
        DAGWebSocketHandler.broadcastMessage(jsonMessage);
    }

    @Override
    public void onNodeStarted(DAGNode<T, R> node) {
        System.out.println("DAGWebSocketListener: Node started: " + node.getId());
        sendStatusUpdate(node, TaskStatus.RUNNING);
    }

    @Override
    public void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {
        System.out.println("DAGWebSocketListener: Node completed: " + node.getId());
        sendStatusUpdate(node, TaskStatus.COMPLETED);
    }

    @Override
    public void onNodeFailed(DAGNode<T, R> node, Exception error) {
        System.out.println("DAGWebSocketListener: Node failed: " + node.getId());
        sendStatusUpdate(node, TaskStatus.FAILED);
    }

    @Override
    public void onDagCompleted(DAGExecutionResult<T, R> result) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "dagCompleted");
        message.put("executionTime", result.getExecutionTime());
        String jsonMessage = gson.toJson(message);
        System.out.println("Sending DAG completion message: " + jsonMessage);
        DAGWebSocketHandler.broadcastMessage(jsonMessage);
        System.out.println("DAG execution completed in " + result.getExecutionTime() + "ms");
    }

    @Override
    public void updateVisualization(DAGNode<T, R> node) {

    }
}
