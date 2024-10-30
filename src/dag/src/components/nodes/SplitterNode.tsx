import React from 'react';
import { Handle, Position } from 'reactflow';
import { SplitSquareHorizontal } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface SplitterNodeData {
  label: string;
  description: string;
  config?: {
    chunkSize: number;
    overlap: number;
  };
}

export const SplitterNode = ({ data }: { data: SplitterNodeData }) => (
  <NodeWrapper className="bg-amber-50/90 border-amber-200">
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} id="a" />
    <Handle type="source" position={Position.Right} id="b" style={{ top: '75%' }} />
    <div className="flex items-center gap-3">
      <SplitSquareHorizontal className="w-6 h-6 text-amber-500" />
      <div>
        <h3 className="font-semibold text-amber-700">{data.label}</h3>
        <p className="text-sm text-amber-600">{data.description}</p>
        {data.config && (
          <div className="mt-2 flex flex-wrap gap-2">
            <span className="inline-block px-2 py-1 bg-amber-100 rounded-md text-xs text-amber-700">
              {data.config.chunkSize} chars
            </span>
            <span className="inline-block px-2 py-1 bg-amber-100 rounded-md text-xs text-amber-700">
              {data.config.overlap} overlap
            </span>
          </div>
        )}
      </div>
    </div>
  </NodeWrapper>
);