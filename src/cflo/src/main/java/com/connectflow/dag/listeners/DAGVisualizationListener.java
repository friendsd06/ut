package com.connectflow.dag.listeners;

import com.connectflow.dag.DAG;
import com.connectflow.dag.DAGNode;
import com.connectflow.dag.ExecutionResult;
import com.connectflow.dag.execution.DAGExecutionResult;
import com.connectflow.dag.tasks.TaskStatus;
import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static guru.nidi.graphviz.model.Factory.*;

public class DAGVisualizationListener<T, R> implements DAGExecutionListener<T, R> {
    private final DAG<T, R> dag;
    private final Map<T, TaskStatus> nodeStatuses;
    private final String outputDir;

    public DAGVisualizationListener(DAG<T, R> dag, String outputDir) {
        this.dag = dag;
        this.nodeStatuses = new ConcurrentHashMap<>();
        this.outputDir = "D:\\dag-visualation";
        new File(outputDir).mkdirs();
    }

    @Override
    public void onNodeStarted(DAGNode<T, R> node) {
        nodeStatuses.put(node.getId(), TaskStatus.RUNNING);
        updateVisualization(node);
    }

    @Override
    public void onNodeCompleted(DAGNode<T, R> node, ExecutionResult<T, R> result) {
        nodeStatuses.put(node.getId(), TaskStatus.COMPLETED);
        updateVisualization(node);
    }

    @Override
    public void onNodeFailed(DAGNode<T, R> node, Exception error) {
        nodeStatuses.put(node.getId(), TaskStatus.FAILED);
        updateVisualization(node);
    }

    @Override
    public void onDagCompleted(DAGExecutionResult<T, R> result) {
        System.out.println("DAG execution completed in " + result.getExecutionTime() + "ms");
    }

    @Override
    public void updateVisualization(DAGNode<T, R> node) {
        try {
            MutableGraph graph = mutGraph("DAG").setDirected(true);

            Map<T, guru.nidi.graphviz.model.MutableNode> graphNodes = new ConcurrentHashMap<>();

            for (DAGNode<T, R> dagNode : dag.getNodes()) {
                guru.nidi.graphviz.model.MutableNode mutableNode = mutNode(dagNode.getId().toString());
                TaskStatus status = nodeStatuses.getOrDefault(dagNode.getId(), TaskStatus.PENDING);
                switch (status) {
                    case PENDING:
                        mutableNode.add(Color.GRAY);
                        break;
                    case RUNNING:
                        mutableNode.add(Color.ORANGE);
                        break;
                    case COMPLETED:
                        mutableNode.add(Color.GREEN);
                        break;
                    case FAILED:
                        mutableNode.add(Color.RED);
                        break;
                    default:
                        mutableNode.add(Color.BLACK);
                        break;
                }
                mutableNode.add(Label.of(dagNode.getId().toString()));
                graphNodes.put(dagNode.getId(), mutableNode);
                graph.add(mutableNode);
            }

            // Add edges
            for (DAGNode<T, R> dagNode : dag.getNodes()) {
                for (DAGNode<T, R> child : dagNode.getOutgoingNodes()) {
                    graphNodes.get(dagNode.getId()).addLink(graphNodes.get(child.getId()));
                }
            }

            // Render the graph to a file
            String fileName = outputDir + "/dag_" + System.currentTimeMillis() + ".png";
            Graphviz.fromGraph(graph).render(Format.PNG).toFile(new File(fileName));
            System.out.println("Visualization updated: " + fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
