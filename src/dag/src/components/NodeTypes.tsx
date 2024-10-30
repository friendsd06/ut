import React from 'react';
import { Handle, Position } from 'reactflow';
import {
  MessageSquare,
  Brain,
  SplitSquareHorizontal,
  Database,
  Combine,
  FileText,
  Send,
  Settings2
} from 'lucide-react';

const baseNodeStyles = `
  p-4 rounded-xl shadow-lg border-2 min-w-[220px]
  transition-all duration-200 hover:shadow-xl
  backdrop-blur-sm backdrop-filter
`;

interface NodeData {
  label: string;
  description: string;
  config?: Record<string, any>;
}

const NodeWrapper = ({ 
  children, 
  className 
}: { 
  children: React.ReactNode;
  className: string;
}) => (
  <div className={`${baseNodeStyles} ${className}`}>
    {children}
  </div>
);

export const PromptNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-indigo-50/90 border-indigo-200">
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <MessageSquare className="w-6 h-6 text-indigo-500" />
      <div>
        <h3 className="font-semibold text-indigo-700">{data.label}</h3>
        <p className="text-sm text-indigo-600">{data.description}</p>
      </div>
    </div>
  </NodeWrapper>
);

export const LLMNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-violet-50/90 border-violet-200">
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <Brain className="w-6 h-6 text-violet-500" />
      <div>
        <h3 className="font-semibold text-violet-700">{data.label}</h3>
        <p className="text-sm text-violet-600">{data.description}</p>
        {data.config?.model && (
          <span className="inline-block mt-1 px-2 py-1 bg-violet-100 rounded-md text-xs text-violet-700">
            {data.config.model}
          </span>
        )}
      </div>
    </div>
  </NodeWrapper>
);

export const SplitterNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-amber-50/90 border-amber-200">
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} id="a" />
    <Handle type="source" position={Position.Right} id="b" style={{ top: '75%' }} />
    <div className="flex items-center gap-3">
      <SplitSquareHorizontal className="w-6 h-6 text-amber-500" />
      <div>
        <h3 className="font-semibold text-amber-700">{data.label}</h3>
        <p className="text-sm text-amber-600">{data.description}</p>
      </div>
    </div>
  </NodeWrapper>
);

export const DataSourceNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-blue-50/90 border-blue-200">
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <Database className="w-6 h-6 text-blue-500" />
      <div>
        <h3 className="font-semibold text-blue-700">{data.label}</h3>
        <p className="text-sm text-blue-600">{data.description}</p>
      </div>
    </div>
  </NodeWrapper>
);

export const CombinerNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-emerald-50/90 border-emerald-200">
    <Handle type="target" position={Position.Left} id="a" />
    <Handle type="target" position={Position.Left} id="b" style={{ top: '75%' }} />
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <Combine className="w-6 h-6 text-emerald-500" />
      <div>
        <h3 className="font-semibold text-emerald-700">{data.label}</h3>
        <p className="text-sm text-emerald-600">{data.description}</p>
      </div>
    </div>
  </NodeWrapper>
);

export const OutputNode = ({ data }: { data: NodeData }) => (
  <NodeWrapper className="bg-green-50/90 border-green-200">
    <Handle type="target" position={Position.Left} />
    <div className="flex items-center gap-3">
      <Send className="w-6 h-6 text-green-500" />
      <div>
        <h3 className="font-semibold text-green-700">{data.label}</h3>
        <p className="text-sm text-green-600">{data.description}</p>
      </div>
    </div>
  </NodeWrapper>
);

export const nodeTypes = {
  prompt: PromptNode,
  llm: LLMNode,
  splitter: SplitterNode,
  datasource: DataSourceNode,
  combiner: CombinerNode,
  output: OutputNode,
};