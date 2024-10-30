import React, { useState } from 'react';

interface PromptEditorProps {
  value: string;
  systemPrompt?: string;
  onChange: (template: string, systemPrompt: string) => void;
}

const VARIABLES = [
  { name: 'input', description: 'Main input text' },
  { name: 'context', description: 'Additional context' },
  { name: 'history', description: 'Conversation history' },
  { name: 'query', description: 'User query' },
  { name: 'user', description: 'User information' }
] as const;

export const PromptEditor: React.FC<PromptEditorProps> = ({ 
  value, 
  systemPrompt = '', 
  onChange 
}) => {
  const [selectedVariable, setSelectedVariable] = useState<string>('');

  const handleInsertVariable = (varName: string) => {
    const textArea = document.getElementById('promptTemplate') as HTMLTextAreaElement;
    if (textArea) {
      const start = textArea.selectionStart;
      const end = textArea.selectionEnd;
      const newValue = `${value.substring(0, start)}{{${varName}}}${value.substring(end)}`;
      onChange(newValue, systemPrompt);
      textArea.focus();
    }
  };

  return (
    <div className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          System Prompt
        </label>
        <textarea
          id="systemPrompt"
          value={systemPrompt}
          onChange={(e) => onChange(value, e.target.value)}
          className="w-full h-24 p-3 border rounded-lg bg-white/50 backdrop-blur-sm font-mono text-sm"
          placeholder="Enter system prompt (optional)"
        />
      </div>
      <div>
        <div className="flex justify-between items-center mb-2">
          <label className="block text-sm font-medium text-gray-700">
            Prompt Template
          </label>
          <div className="flex gap-2">
            {VARIABLES.map(({ name }) => (
              <button
                key={name}
                onClick={() => handleInsertVariable(name)}
                className="px-2 py-1 text-xs bg-indigo-100 text-indigo-700 rounded-md 
                  hover:bg-indigo-200 transition-colors"
              >
                {name}
              </button>
            ))}
          </div>
        </div>
        <textarea
          id="promptTemplate"
          value={value}
          onChange={(e) => onChange(e.target.value, systemPrompt)}
          className="w-full h-40 p-3 border rounded-lg bg-white/50 backdrop-blur-sm font-mono text-sm"
          placeholder="Enter your prompt template here..."
        />
        <div className="mt-2 space-y-2">
          <p className="text-xs text-gray-500">
            Use {'{{variable}}'} syntax for dynamic values, e.g., "Answer this: {'{{query}}'}"
          </p>
          <div className="text-xs text-gray-600">
            Available variables:
            <ul className="mt-1 space-y-1">
              {VARIABLES.map(({ name, description }) => (
                <li key={name} className="flex items-center gap-2">
                  <code className="px-1 bg-gray-100 rounded">{'{{' + name + '}}'}</code>
                  <span>- {description}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};