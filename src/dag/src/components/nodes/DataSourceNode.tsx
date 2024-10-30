import React from 'react';
import { Handle, Position } from 'reactflow';
import { Database } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface DataSourceNodeData {
  label: string;
  description: string;
  config?: {
    type: string;
    url?: string;
  };
}

export const DataSourceNode = ({ data }: { data: DataSourceNodeData }) => (
  <NodeWrapper className="bg-blue-50/90 border-blue-200">
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <Database className="w-6 h-6 text-blue-500" />
      <div>
        <h3 className="font-semibold text-blue-700">{data.label}</h3>
        <p className="text-sm text-blue-600">{data.description}</p>
        {data.config && (
          <div className="mt-2 flex flex-wrap gap-2">
            <span className="inline-block px-2 py-1 bg-blue-100 rounded-md text-xs text-blue-700">
              {data.config.type}
            </span>
            {data.config.url && (
              <span className="inline-block px-2 py-1 bg-blue-100 rounded-md text-xs text-blue-700">
                {data.config.url}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  </NodeWrapper>
);