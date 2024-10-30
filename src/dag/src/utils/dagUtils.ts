import { Node, Edge, Connection } from 'reactflow';

export const doesCreateCycle = (
  nodes: Node[],
  edges: Edge[],
  newConnection: Connection
): boolean => {
  const graph = new Map<string, string[]>();
  nodes.forEach((node) => graph.set(node.id, []));
  edges.forEach((edge) => {
    const sources = graph.get(edge.source) || [];
    sources.push(edge.target);
    graph.set(edge.source, sources);
  });

  if (newConnection.source && newConnection.target) {
    const sources = graph.get(newConnection.source) || [];
    sources.push(newConnection.target);
    graph.set(newConnection.source, sources);
  }

  const visited = new Set<string>();
  const recursionStack = new Set<string>();

  const hasCycle = (node: string): boolean => {
    if (!visited.has(node)) {
      visited.add(node);
      recursionStack.add(node);

      const neighbors = graph.get(node) || [];
      for (const neighbor of neighbors) {
        if (
          !visited.has(neighbor) && hasCycle(neighbor) ||
          recursionStack.has(neighbor)
        ) {
          return true;
        }
      }
    }
    recursionStack.delete(node);
    return false;
  };

  for (const node of nodes) {
    if (hasCycle(node.id)) {
      return true;
    }
  }

  return false;
};

export const getDefaultDescription = (type: string): string => {
  const descriptions: Record<string, string> = {
    prompt: 'Template for generating prompts',
    llm: 'Process text with language model',
    splitter: 'Split text into chunks',
    datasource: 'Load data from source',
    combiner: 'Combine multiple inputs',
    output: 'Pipeline output',
    vectorstore: 'Vector database for embeddings',
  };
  return descriptions[type] || '';
};

export const getDefaultConfig = (type: string): Record<string, any> => {
  const configs: Record<string, any> = {
    llm: { 
      model: 'gpt-3.5-turbo', 
      temperature: 0.7,
      maxTokens: 2048,
      topP: 1,
      frequencyPenalty: 0,
      presencePenalty: 0
    },
    splitter: { 
      chunkSize: 1000, 
      overlap: 200,
      splitBy: 'tokens'
    },
    vectorstore: { 
      type: 'pinecone', 
      index: 'default',
      namespace: 'default',
      metric: 'cosine'
    },
    prompt: { 
      template: '',
      systemPrompt: '',
      variables: []
    },
    datasource: { 
      type: 'pdf',
      format: 'text',
      encoding: 'utf-8'
    },
    combiner: {
      mode: 'concatenate',
      separator: '\n\n'
    },
    output: {
      format: 'text',
      saveToFile: false
    }
  };
  return configs[type] || {};
};