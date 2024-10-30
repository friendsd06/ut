import { PromptNode } from './PromptNode';
import { LLMNode } from './LLMNode';
import { SplitterNode } from './SplitterNode';
import { DataSourceNode } from './DataSourceNode';
import { CombinerNode } from './CombinerNode';
import { OutputNode } from './OutputNode';
import { VectorStoreNode } from './VectorStoreNode';

export const nodeTypes = {
  prompt: PromptNode,
  llm: LLMNode,
  splitter: SplitterNode,
  datasource: DataSourceNode,
  combiner: CombinerNode,
  output: OutputNode,
  vectorstore: VectorStoreNode,
};