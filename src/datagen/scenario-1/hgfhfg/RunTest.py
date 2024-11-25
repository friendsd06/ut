import unittest
from typing import List, Dict, Tuple
import logging

from demo4 import TaskProcessor

logging.getLogger().setLevel(logging.ERROR)  # Reduce logging noise during tests


class TestTaskProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = TaskProcessor()

    def test_basic_pipeline(self):
        """Test basic sequential pipeline"""
        metadata = [
            {'taskid': 'A', 'operator_type': 'PythonOperator'},
            {'taskid': 'B', 'operator_type': 'PythonOperator'},
            {'taskid': 'C', 'operator_type': 'PythonOperator'}
        ]

        pipeline = [
            {'pipelineid': 'basic', 'taskid': 'A', 'dependenttaskid': None},
            {'pipelineid': 'basic', 'taskid': 'B', 'dependenttaskid': 'A'},
            {'pipelineid': 'basic', 'taskid': 'C', 'dependenttaskid': 'B'}
        ]

        results = self.processor.process_pipeline(pipeline, metadata)
        print(results)
        self.assertEqual(results['basic']['status'], 'success')
        self.assertEqual(results['basic']['root_tasks'], ['A'])
        self.assertEqual(len(results['basic']['dependencies']), 2)

    def test_branch_operator(self):
        """Test branch operator functionality"""
        metadata = [
            {'taskid': 'check', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'success', 'operator_type': 'PythonOperator'},
            {'taskid': 'failure', 'operator_type': 'PythonOperator'}
        ]

        pipeline = [
            {'pipelineid': 'branch', 'taskid': 'check', 'dependenttaskid': None},
            {'pipelineid': 'branch', 'taskid': 'success', 'dependenttaskid': 'check'},
            {'pipelineid': 'branch', 'taskid': 'failure', 'dependenttaskid': 'check'}
        ]

        results = self.processor.process_pipeline(pipeline, metadata)
        self.assertIn('check >> [failure, success]', results['branch']['dependencies'])

    def test_parallel_tasks(self):
        """Test parallel task execution"""
        metadata = [
            {'taskid': 'start', 'operator_type': 'PythonOperator'},
            {'taskid': 'parallel1', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},
            {'taskid': 'parallel2', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},
            {'taskid': 'end', 'operator_type': 'PythonOperator'}
        ]

        pipeline = [
            {'pipelineid': 'parallel', 'taskid': 'start', 'dependenttaskid': None},
            {'pipelineid': 'parallel', 'taskid': 'parallel1', 'dependenttaskid': 'start'},
            {'pipelineid': 'parallel', 'taskid': 'parallel2', 'dependenttaskid': 'start'},
            {'pipelineid': 'parallel', 'taskid': 'end', 'dependenttaskid': 'parallel1'},
            {'pipelineid': 'parallel', 'taskid': 'end', 'dependenttaskid': 'parallel2'}
        ]

        results = self.processor.process_pipeline(pipeline, metadata)
        self.assertIn('start >> [parallel1, parallel2]', results['parallel']['dependencies'])

    def test_cycle_detection(self):
        """Test cycle detection"""
        metadata = [
            {'taskid': 'A', 'operator_type': 'PythonOperator'},
            {'taskid': 'B', 'operator_type': 'PythonOperator'},
            {'taskid': 'C', 'operator_type': 'PythonOperator'}
        ]

        pipeline = [
            {'pipelineid': 'cycle', 'taskid': 'A', 'dependenttaskid': 'C'},
            {'pipelineid': 'cycle', 'taskid': 'B', 'dependenttaskid': 'A'},
            {'pipelineid': 'cycle', 'taskid': 'C', 'dependenttaskid': 'B'}
        ]

        results = self.processor.process_pipeline(pipeline, metadata)
        self.assertEqual(results['cycle']['status'], 'error')
        self.assertTrue(any('cycle' in err.lower() for err in results['cycle']['errors']))

    def test_invalid_input(self):
        """Test invalid input handling"""
        test_cases = [
            # Empty pipeline
            ([], [], 'Pipeline data cannot be empty'),

            # Missing required fields
            ([{'pipelineid': 'test'}], [], 'Missing required fields'),

            # Invalid operator type
            (
                [{'pipelineid': 'test', 'taskid': 'A', 'dependenttaskid': None}],
                [{'taskid': 'A', 'operator_type': 'InvalidOperator'}],
                'Invalid operator type'
            )
        ]

        for pipeline, metadata, expected_error in test_cases:
            with self.subTest(case=expected_error):
                try:
                    self.processor.process_pipeline(pipeline, metadata)
                    self.fail(f"Expected error containing: {expected_error}")
                except Exception as e:
                    self.assertIn(expected_error.lower(), str(e).lower())

    def test_complex_pipeline(self):
        """Test complex pipeline with multiple operators and dependencies"""
        metadata = [
            {'taskid': 'extract', 'operator_type': 'PythonOperator'},
            {'taskid': 'validate', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'process_valid', 'operator_type': 'PythonOperator'},
            {'taskid': 'process_invalid', 'operator_type': 'PythonOperator'},
            {'taskid': 'notify_success', 'operator_type': 'EmailOperator'},
            {'taskid': 'notify_failure', 'operator_type': 'EmailOperator'}
        ]

        pipeline = [
            {'pipelineid': 'complex', 'taskid': 'extract', 'dependenttaskid': None},
            {'pipelineid': 'complex', 'taskid': 'validate', 'dependenttaskid': 'extract'},
            {'pipelineid': 'complex', 'taskid': 'process_valid', 'dependenttaskid': 'validate'},
            {'pipelineid': 'complex', 'taskid': 'process_invalid', 'dependenttaskid': 'validate'},
            {'pipelineid': 'complex', 'taskid': 'notify_success', 'dependenttaskid': 'process_valid'},
            {'pipelineid': 'complex', 'taskid': 'notify_failure', 'dependenttaskid': 'process_invalid'}
        ]

        results = self.processor.process_pipeline(pipeline, metadata)
        self.assertEqual(results['complex']['status'], 'success')
        self.assertEqual(results['complex']['root_tasks'], ['extract'])
        self.assertTrue(any('validate >> [process_invalid, process_valid]' in dep
                            for dep in results['complex']['dependencies']))

    def test_empty_pipeline(self):
        """Test handling of empty pipeline"""
        metadata = [{'taskid': 'A', 'operator_type': 'PythonOperator'}]
        pipeline = [{'pipelineid': 'empty', 'taskid': 'A', 'dependenttaskid': None}]

        results = self.processor.process_pipeline(pipeline, metadata)
        self.assertEqual(results['empty']['root_tasks'], ['A'])
        self.assertEqual(results['empty']['dependencies'], [])


def create_test_data() -> Tuple[List[Dict], List[Dict]]:
    """Create comprehensive test data"""
    task_metadata = [
        # Branch operators
        {'taskid': 'check_data', 'operator_type': 'BranchPythonOperator'},
        {'taskid': 'validate_data', 'operator_type': 'BranchPythonOperator'},

        # Processing tasks
        {'taskid': 'extract_data', 'operator_type': 'PythonOperator'},
        {'taskid': 'transform_data', 'operator_type': 'PythonOperator'},
        {'taskid': 'load_data', 'operator_type': 'PythonOperator'},

        # Parallel tasks
        {'taskid': 'process_chunk1', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},
        {'taskid': 'process_chunk2', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},

        # Notification tasks
        {'taskid': 'send_success', 'operator_type': 'EmailOperator'},
        {'taskid': 'send_failure', 'operator_type': 'EmailOperator'}
    ]

    pipeline_data = [
        # Main flow
        {'pipelineid': 'test_pipeline', 'taskid': 'extract_data', 'dependenttaskid': None},
        {'pipelineid': 'test_pipeline', 'taskid': 'check_data', 'dependenttaskid': 'extract_data'},

        # Success path
        {'pipelineid': 'test_pipeline', 'taskid': 'transform_data', 'dependenttaskid': 'check_data'},
        {'pipelineid': 'test_pipeline', 'taskid': 'process_chunk1', 'dependenttaskid': 'transform_data'},
        {'pipelineid': 'test_pipeline', 'taskid': 'process_chunk2', 'dependenttaskid': 'transform_data'},
        {'pipelineid': 'test_pipeline', 'taskid': 'load_data', 'dependenttaskid': 'process_chunk1'},
        {'pipelineid': 'test_pipeline', 'taskid': 'load_data', 'dependenttaskid': 'process_chunk2'},
        {'pipelineid': 'test_pipeline', 'taskid': 'send_success', 'dependenttaskid': 'load_data'},

        # Failure path
        {'pipelineid': 'test_pipeline', 'taskid': 'send_failure', 'dependenttaskid': 'check_data'}
    ]

    return task_metadata, pipeline_data


def main():
    """Run all tests"""
    unittest.main(verbosity=2)


if __name__ == '__main__':
    main()