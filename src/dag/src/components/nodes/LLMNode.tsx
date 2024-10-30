import React from 'react';
import { Handle, Position } from 'reactflow';
import { Brain } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface LLMNodeData {
  label: string;
  description: string;
  config?: {
    model: string;
    temperature: number;
    maxTokens?: number;
    topP?: number;
    frequencyPenalty?: number;
    presencePenalty?: number;
  };
}

export const LLMNode = ({ data }: { data: LLMNodeData }) => (
  <NodeWrapper className="bg-violet-50/90 border-violet-200 !min-w-[160px]">
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-2">
      <Brain className="w-4 h-4 text-violet-500" />
      <div>
        <h3 className="font-semibold text-violet-700 text-xs">{data.label}</h3>
        <p className="text-[10px] text-violet-600">{data.description}</p>
        {data.config && (
          <div className="mt-1 flex flex-wrap gap-1">
            <span className="px-1 py-0.5 bg-violet-100 rounded text-[9px] text-violet-700">
              {data.config.model}
            </span>
            <span className="px-1 py-0.5 bg-violet-100 rounded text-[9px] text-violet-700">
              {data.config.temperature} temp
            </span>
          </div>
        )}
      </div>
    </div>
  </NodeWrapper>
);