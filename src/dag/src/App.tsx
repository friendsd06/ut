import React from 'react';
import { ReactFlowProvider } from 'reactflow';
import { DAGEditor } from './components/DAGEditor';
import { Sidebar } from './components/Sidebar';
import 'reactflow/dist/style.css';

function App() {
  return (
    <ReactFlowProvider>
      <div className="flex h-screen bg-gray-50">
        <Sidebar />
        <div className="flex-grow">
          <DAGEditor />
        </div>
      </div>
    </ReactFlowProvider>
  );
}

export default App;