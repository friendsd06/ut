import React from 'react';
import { Handle, Position } from 'reactflow';
import { Combine } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface CombinerNodeData {
  label: string;
  description: string;
}

export const CombinerNode = ({ data }: { data: CombinerNodeData }) => (
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