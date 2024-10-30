import React from 'react';
import { Handle, Position } from 'reactflow';
import { Database } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface VectorStoreNodeData {
  label: string;
  description: string;
  config?: {
    type: string;
    index: string;
  };
}

export const VectorStoreNode = ({ data }: { data: VectorStoreNodeData }) => (
  <NodeWrapper className="bg-purple-50/90 border-purple-200">
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <Database className="w-6 h-6 text-purple-500" />
      <div>
        <h3 className="font-semibold text-purple-700">{data.label}</h3>
        <p className="text-sm text-purple-600">{data.description}</p>
        {data.config && (
          <div className="mt-2 flex flex-wrap gap-2">
            <span className="inline-block px-2 py-1 bg-purple-100 rounded-md text-xs text-purple-700">
              {data.config.type}
            </span>
            <span className="inline-block px-2 py-1 bg-purple-100 rounded-md text-xs text-purple-700">
              {data.config.index}
            </span>
          </div>
        )}
      </div>
    </div>
  </NodeWrapper>
);