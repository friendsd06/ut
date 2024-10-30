import React from 'react';

interface ModelSelectorProps {
  value: string;
  onChange: (value: string) => void;
}

export const ModelSelector: React.FC<ModelSelectorProps> = ({ value, onChange }) => (
  <select
    value={value}
    onChange={(e) => onChange(e.target.value)}
    className="w-full p-2 border rounded-lg bg-white/50 backdrop-blur-sm"
  >
    <option value="gpt-4">GPT-4</option>
    <option value="gpt-3.5-turbo">GPT-3.5 Turbo</option>
    <option value="claude-2">Claude 2</option>
    <option value="llama-2">Llama 2</option>
  </select>
);