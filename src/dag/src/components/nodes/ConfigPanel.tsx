import React from 'react';
import { Settings2, X } from 'lucide-react';
import { ModelSelector } from './ModelSelector';
import { PromptEditor } from './PromptEditor';

interface ConfigPanelProps {
  isOpen: boolean;
  onClose: () => void;
  nodeData: any;
  onUpdate: (data: any) => void;
  type: string;
}

export const ConfigPanel: React.FC<ConfigPanelProps> = ({ 
  isOpen, 
  onClose, 
  nodeData, 
  onUpdate, 
  type 
}) => {
  if (!isOpen) return null;

  const renderConfig = () => {
    switch (type) {
      case 'llm':
        return (
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Model</label>
              <ModelSelector
                value={nodeData.config?.model || 'gpt-3.5-turbo'}
                onChange={(model) => onUpdate({
                  ...nodeData,
                  config: { ...nodeData.config, model }
                })}
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Temperature</label>
              <input
                type="range"
                min="0"
                max="1"
                step="0.1"
                value={nodeData.config?.temperature || 0.7}
                onChange={(e) => onUpdate({
                  ...nodeData,
                  config: { ...nodeData.config, temperature: parseFloat(e.target.value) }
                })}
                className="w-full"
              />
              <div className="text-xs text-gray-500 text-right">
                {nodeData.config?.temperature || 0.7}
              </div>
            </div>
          </div>
        );
      case 'prompt':
        return (
          <PromptEditor
            value={nodeData.config?.template || ''}
            systemPrompt={nodeData.config?.systemPrompt || ''}
            onChange={(template, systemPrompt) => {
              const variables = (template.match(/\{\{([^}]+)\}\}/g) || [])
                .map(v => v.slice(2, -2));
              onUpdate({
                ...nodeData,
                config: { 
                  ...nodeData.config, 
                  template,
                  systemPrompt,
                  variables
                }
              });
            }}
          />
        );
      default:
        return null;
    }
  };

  return (
    <div className="absolute right-0 top-0 w-96 h-full bg-white/95 backdrop-blur-sm border-l border-gray-200 shadow-xl p-6 overflow-y-auto">
      <div className="flex justify-between items-center mb-6">
        <div className="flex items-center gap-2">
          <Settings2 className="w-5 h-5 text-gray-600" />
          <h3 className="text-lg font-semibold text-gray-800">Configure Node</h3>
        </div>
        <button
          onClick={onClose}
          className="p-1 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <X className="w-5 h-5 text-gray-600" />
        </button>
      </div>
      {renderConfig()}
    </div>
  );
};