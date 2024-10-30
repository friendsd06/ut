import React from 'react';
import {
  MessageSquare,
  Brain,
  SplitSquareHorizontal,
  Database,
  Combine,
  Send,
  HardDrive
} from 'lucide-react';

interface NodeTemplateProps {
  type: string;
  icon: React.ElementType;
  label: string;
  color: string;
  description: string;
}

const NodeTemplate = ({ type, icon: Icon, label, color, description }: NodeTemplateProps) => (
  <div
    className={`flex flex-col gap-2 p-4 mb-3 rounded-xl cursor-move
      bg-${color}-50/90 border-2 border-${color}-200 
      hover:shadow-md transition-all hover:scale-[1.02]`}
    onDragStart={(e) => {
      e.dataTransfer.setData('application/reactflow', type);
      e.dataTransfer.effectAllowed = 'move';
    }}
    draggable
  >
    <div className="flex items-center gap-3">
      <Icon className={`w-5 h-5 text-${color}-500`} />
      <span className={`text-${color}-700 font-medium`}>{label}</span>
    </div>
    <p className={`text-sm text-${color}-600 ml-8`}>{description}</p>
  </div>
);

export const Sidebar = () => {
  return (
    <div className="w-80 bg-white/80 backdrop-blur-sm p-6 border-r border-gray-200 h-full overflow-y-auto">
      <div className="mb-6">
        <h2 className="text-xl font-bold text-gray-800 mb-2">LLM Pipeline Nodes</h2>
        <p className="text-sm text-gray-600">Drag nodes to the canvas to build your LLM pipeline</p>
      </div>
      
      <NodeTemplate
        type="prompt"
        icon={MessageSquare}
        label="Prompt Template"
        color="indigo"
        description="Define prompt templates with variables"
      />
      <NodeTemplate
        type="llm"
        icon={Brain}
        label="LLM"
        color="violet"
        description="Process text using language models"
      />
      <NodeTemplate
        type="vectorstore"
        icon={HardDrive}
        label="Vector Store"
        color="purple"
        description="Store and query vector embeddings"
      />
      <NodeTemplate
        type="splitter"
        icon={SplitSquareHorizontal}
        label="Text Splitter"
        color="amber"
        description="Split text into smaller chunks"
      />
      <NodeTemplate
        type="datasource"
        icon={Database}
        label="Data Source"
        color="blue"
        description="Load data from various sources"
      />
      <NodeTemplate
        type="combiner"
        icon={Combine}
        label="Combiner"
        color="emerald"
        description="Merge multiple inputs into one"
      />
      <NodeTemplate
        type="output"
        icon={Send}
        label="Output"
        color="green"
        description="Final output of the pipeline"
      />
    </div>
  );
};