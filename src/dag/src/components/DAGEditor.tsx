import React, { useState, useCallback, useRef } from 'react';
import ReactFlow, {
  Background,
  Controls,
  Connection,
  Edge,
  Node,
  addEdge,
  useNodesState,
  useEdgesState,
  ReactFlowProvider,
  Panel,
  MiniMap,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { nodeTypes } from './nodes/NodeTypes';
import { ConfigPanel } from './nodes/ConfigPanel';
import { Toolbar } from './Toolbar';

let id = 0;
const getId = () => `node_${id++}`;

const defaultEdgeOptions = {
  animated: true,
  style: {
    stroke: '#94a3b8',
    strokeWidth: 2,
  },
};

export const DAGEditor = () => {
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [configOpen, setConfigOpen] = useState(false);
  const [showMinimap, setShowMinimap] = useState(false);

  const onConnect = useCallback(
    (params: Connection) => {
      setEdges((eds) => addEdge(params, eds));
    },
    [setEdges]
  );

  const onNodeDoubleClick = useCallback((event: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
    setConfigOpen(true);
  }, []);

  const updateNodeData = useCallback((newData: any) => {
    if (!selectedNode) return;
    setNodes((nds) =>
      nds.map((node) =>
        node.id === selectedNode.id ? { ...node, data: { ...node.data, ...newData } } : node
      )
    );
  }, [selectedNode, setNodes]);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow');
      if (!type) return;

      const position = reactFlowWrapper.current?.getBoundingClientRect();
      if (!position) return;

      const newNode: Node = {
        id: getId(),
        type,
        position: {
          x: event.clientX - position.left - 75,
          y: event.clientY - position.top - 25,
        },
        data: {
          label: `${type.charAt(0).toUpperCase() + type.slice(1)} Node`,
          description: getDefaultDescription(type),
          config: getDefaultConfig(type),
        },
        style: {
          width: type === 'llm' ? 160 : 220,
          height: 'auto',
        },
      };

      setNodes((nds) => nds.concat(newNode));
    },
    [setNodes]
  );

  return (
    <div className="flex flex-col h-full">
      <Toolbar
        nodes={nodes}
        edges={edges}
        setNodes={setNodes}
        setEdges={setEdges}
        setError={setError}
        showMinimap={showMinimap}
        setShowMinimap={setShowMinimap}
      />
      {error && (
        <div className="p-4 bg-red-100 text-red-700 border-b border-red-200">
          {error}
        </div>
      )}
      <div ref={reactFlowWrapper} className="flex-grow relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onDrop={onDrop}
          onDragOver={onDragOver}
          onNodeDoubleClick={onNodeDoubleClick}
          nodeTypes={nodeTypes}
          defaultEdgeOptions={defaultEdgeOptions}
          snapToGrid
          snapGrid={[20, 20]}
        >
          <Background />
          <Controls />
          {showMinimap && (
            <MiniMap
              nodeColor={(node) => {
                switch (node.type) {
                  case 'prompt':
                    return '#818cf8';
                  case 'llm':
                    return '#8b5cf6';
                  case 'splitter':
                    return '#f59e0b';
                  case 'datasource':
                    return '#3b82f6';
                  case 'combiner':
                    return '#10b981';
                  case 'output':
                    return '#22c55e';
                  default:
                    return '#94a3b8';
                }
              }}
              maskColor="rgba(255, 255, 255, 0.8)"
            />
          )}
          <Panel position="bottom-left" className="bg-white/90 p-2 rounded-lg shadow-sm">
            <div className="text-sm text-gray-600">
              Nodes: {nodes.length} | Edges: {edges.length}
            </div>
          </Panel>
        </ReactFlow>
        {selectedNode && (
          <ConfigPanel
            isOpen={configOpen}
            onClose={() => setConfigOpen(false)}
            nodeData={selectedNode.data}
            onUpdate={updateNodeData}
            type={selectedNode.type as string}
          />
        )}
      </div>
    </div>
  );
};

const getDefaultDescription = (type: string): string => {
  const descriptions: Record<string, string> = {
    prompt: 'Template for generating prompts',
    llm: 'Process text with language model',
    splitter: 'Split text into chunks',
    datasource: 'Load data from source',
    combiner: 'Combine multiple inputs',
    output: 'Pipeline output',
    vectorstore: 'Vector database for embeddings',
  };
  return descriptions[type] || '';
};

const getDefaultConfig = (type: string): Record<string, any> => {
  const configs: Record<string, any> = {
    llm: { 
      model: 'gpt-3.5-turbo', 
      temperature: 0.7,
      maxTokens: 2048,
      topP: 1,
      frequencyPenalty: 0,
      presencePenalty: 0
    },
    splitter: { 
      chunkSize: 1000, 
      overlap: 200,
      splitBy: 'tokens'
    },
    vectorstore: { 
      type: 'pinecone', 
      index: 'default',
      namespace: 'default',
      metric: 'cosine'
    },
    prompt: { 
      template: '',
      systemPrompt: '',
      variables: []
    },
    datasource: { 
      type: 'pdf',
      format: 'text',
      encoding: 'utf-8'
    },
    combiner: {
      mode: 'concatenate',
      separator: '\n\n'
    },
    output: {
      format: 'text',
      saveToFile: false
    }
  };
  return configs[type] || {};
};