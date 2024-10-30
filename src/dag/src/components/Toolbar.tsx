import React from 'react';
import { Node, Edge } from 'reactflow';
import { Play, Save, Trash2, Download, Upload, History, Map } from 'lucide-react';
import axios from 'axios';

interface ToolbarProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  setError: (error: string | null) => void;
  showMinimap: boolean;
  setShowMinimap: (show: boolean) => void;
}

export const Toolbar = ({
  nodes,
  edges,
  setNodes,
  setEdges,
  setError,
  showMinimap,
  setShowMinimap,
}: ToolbarProps) => {
  const [isExecuting, setIsExecuting] = React.useState(false);

  const executeDAG = async () => {
    try {
      setIsExecuting(true);
      setError(null);
      const response = await axios.post('/api/execute-pipeline', { nodes, edges });
      console.log('Pipeline Execution Result:', response.data);
    } catch (err: any) {
      setError(err.message || 'Failed to execute pipeline');
    } finally {
      setIsExecuting(false);
    }
  };

  const exportPipeline = () => {
    const data = { nodes, edges };
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'llm-pipeline.json';
    a.click();
    URL.revokeObjectURL(url);
  };

  const importPipeline = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = JSON.parse(e.target?.result as string);
          setNodes(data.nodes || []);
          setEdges(data.edges || []);
        } catch (err) {
          setError('Failed to import pipeline');
        }
      };
      reader.readAsText(file);
    }
  };

  const clearCanvas = () => {
    setNodes([]);
    setEdges([]);
    setError(null);
  };

  return (
    <div className="flex justify-between items-center p-4 bg-white/80 backdrop-blur-sm border-b border-gray-200">
      <h1 className="text-2xl font-bold text-gray-800">LLM Pipeline Editor</h1>
      <div className="flex gap-3">
        <input
          type="file"
          id="import-pipeline"
          className="hidden"
          accept=".json"
          onChange={importPipeline}
        />
        <label
          htmlFor="import-pipeline"
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg
            hover:bg-gray-200 transition-colors cursor-pointer"
        >
          <Upload className="w-4 h-4" />
          Import
        </label>
        <button
          onClick={exportPipeline}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg
            hover:bg-gray-200 transition-colors"
        >
          <Download className="w-4 h-4" />
          Export
        </button>
        <button
          onClick={clearCanvas}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg
            hover:bg-gray-200 transition-colors"
        >
          <Trash2 className="w-4 h-4" />
          Clear
        </button>
        <button
          onClick={() => setShowMinimap(!showMinimap)}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors
            ${showMinimap ? 'bg-violet-100 text-violet-700' : 'bg-gray-100 text-gray-700'}`}
        >
          <Map className="w-4 h-4" />
          Minimap
        </button>
        <button
          onClick={executeDAG}
          disabled={isExecuting || nodes.length === 0}
          className="flex items-center gap-2 px-4 py-2 bg-violet-600 text-white rounded-lg
            hover:bg-violet-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Play className="w-4 h-4" />
          {isExecuting ? 'Executing...' : 'Execute Pipeline'}
        </button>
      </div>
    </div>
  );
};