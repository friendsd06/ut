import React from 'react';
import { Handle, Position } from 'reactflow';
import { MessageSquare } from 'lucide-react';
import { NodeWrapper } from './NodeWrapper';

interface PromptNodeData {
  label: string;
  description: string;
  config?: {
    template: string;
    systemPrompt?: string;
    variables?: string[];
  };
}

export const PromptNode = ({ data }: { data: PromptNodeData }) => (
  <NodeWrapper className="bg-indigo-50/90 border-indigo-200">
    <Handle type="source" position={Position.Right} />
    <div className="flex items-center gap-3">
      <MessageSquare className="w-5 h-5 text-indigo-500" />
      <div>
        <h3 className="font-semibold text-indigo-700">{data.label}</h3>
        <p className="text-sm text-indigo-600">{data.description}</p>
        {data.config?.template && (
          <div className="mt-2 space-y-2">
            {data.config.systemPrompt && (
              <div className="p-2 bg-indigo-100/30 rounded-md">
                <div className="text-xs font-medium text-indigo-600 mb-1">System:</div>
                <code className="text-xs text-indigo-700 font-mono break-all">
                  {data.config.systemPrompt.length > 50 
                    ? `${data.config.systemPrompt.slice(0, 50)}...` 
                    : data.config.systemPrompt}
                </code>
              </div>
            )}
            <div className="p-2 bg-indigo-100/50 rounded-md">
              <div className="text-xs font-medium text-indigo-600 mb-1">Template:</div>
              <code className="text-xs text-indigo-700 font-mono break-all">
                {data.config.template.length > 50 
                  ? `${data.config.template.slice(0, 50)}...` 
                  : data.config.template}
              </code>
            </div>
            {data.config.variables && data.config.variables.length > 0 && (
              <div className="flex flex-wrap gap-1">
                {data.config.variables.map((variable) => (
                  <span 
                    key={variable} 
                    className="px-1.5 py-0.5 bg-indigo-200/50 rounded text-xs text-indigo-700"
                  >
                    {`{{${variable}}}`}
                  </span>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  </NodeWrapper>
);