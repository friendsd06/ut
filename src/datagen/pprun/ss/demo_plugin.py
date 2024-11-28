from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
import logging as log
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.fs_hook import FSHook
import os, stat

class DataTransferOperator(BaseOperator):

    @apply_defaults
    def __init__(self, source_file_path, dest_file_path, delete_list, *args, **kwargs):

        self.source_file_path = source_file_path
        self.dest_file_path = dest_file_path
        self.delete_list = delete_list
        super().__init__(*args, **kwargs)

    def execute(self, context):

        SourceFile = self.source_file_path
        DestinationFile = self.dest_file_path
        DeleteList = self.delete_list

        log.info("### custom operator execution starts ###")
        log.info('source_file_path: %s', SourceFile)
        log.info('dest_file_path: %s', DestinationFile)
        log.info('delete_list: %s', DeleteList)

        fin = open(SourceFile)
        fout = open(DestinationFile, "a")

        for line in fin:
            log.info('### reading line: %s', line)
            for word in DeleteList:
                log.info('### matching string: %s', word)
                line = line.replace(word, "")

            log.info('### output line is: %s', line)
            fout.write(line)

        fin.close()
        fout.close()

class FileCountSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, dir_path, conn_id, *args, **kwargs):
        self.dir_path = dir_path
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def poke(self,context):
        hook = FSHook(self.conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.dir_path)
        self.log.info('poking location %s', full_path)
        try:
            for root, dirs, files in os.walk(full_path):
                #print(len(files))
                if len(files) >= 5:
                    return True
        except OSError:
            return False
        return False

class DemoPlugin(AirflowPlugin):
    name = "demo_plugin"
    operators = [DataTransferOperator]
    sensors = [FileCountSensor]
