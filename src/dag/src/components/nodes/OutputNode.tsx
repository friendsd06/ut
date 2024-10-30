import React from 'react';
import { Handle, Position } from 'reactflow';
import { Send } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface OutputNodeData {
  label: string;
  description: string;
}

export const OutputNode = ({ data }: { data: OutputNodeData }) => (
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