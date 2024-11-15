CREATE TABLE PipelineTemplate (
    TemplateID SERIAL PRIMARY KEY,
    TemplateName VARCHAR(255) NOT NULL,
    Description TEXT,                              -- Description of the template’s purpose
    DefaultRetryCount INT DEFAULT 3,               -- Default retry count for tasks in the template
    DefaultRetryDelay INTERVAL DEFAULT INTERVAL '5 minutes', -- Default retry delay for tasks
    DefaultExecutionTimeout INTERVAL,              -- Default execution timeout for tasks in the template
    TaskSpecificMetadata JSONB,                    -- JSON formatted metadata for parameters like batch size, chunk size
    Version VARCHAR(10) DEFAULT '1.0',             -- Template version for tracking changes
    IsActive BOOLEAN DEFAULT TRUE                  -- Flag to indicate if the template is active
);



Enhanced PipelineTemplate Table
The PipelineTemplate table stores predefined templates that define the general structure, configurations, and behavior of pipelines. This table serves as a blueprint for creating and configuring individual pipelines dynamically.

Additional Attributes:

Description: Provides a brief description of the template’s purpose.
DefaultRetryCount and DefaultRetryDelay: Specify default retry policies for tasks within the template.
DefaultExecutionTimeout: Specifies the default maximum execution time for tasks based on the template.
TaskSpecificMetadata: Stores task-level configuration parameters in JSON format (e.g., batch size, chunk size).
Version: To track changes in template versions, allowing for updates and backward compatibility.
IsActive: A boolean field to enable or disable a template for use.



Usage in Airflow:

Default Retry Policies: DefaultRetryCount and DefaultRetryDelay provide default retry settings for tasks in the pipeline, which can be overridden at the pipeline level.
Execution Timeout: DefaultExecutionTimeout specifies the maximum runtime for tasks, ensuring they do not exceed SLA requirements.
Task-Specific Metadata: TaskSpecificMetadata can store JSON parameters like {"batch_size": 1000, "timeout": "5 minutes"}, used in Jinja templates to configure task settings dynamically.
Version Control: Version allows tracking changes and maintaining compatibility with previous versions.


============================================================================

Enhanced Pipeline Table
The Pipeline table represents individual pipelines created from templates, defining schedules, retry policies, dependencies, and execution windows. Each pipeline corresponds to a DAG in Airflow and inherits configurations from its associated template.

Additional Attributes:

Description: A brief description of the pipeline’s purpose and scope.
AlertEmail: An email address for sending alerts or notifications related to the pipeline’s success or failure.
Owner: The person or team responsible for the pipeline.
LastRunStatus and LastRunTimestamp: To track the status and time of the last run, which is useful for monitoring.
MaxParallelTasks: Limits the number of tasks that can run concurrently within the pipeline.
PipelineParameters: JSON field to store pipeline-level parameters specific to this instance (e.g., environment variables, custom configurations).
Priority: An integer field to prioritize pipelines for execution in environments with constrained resources.


CREATE TABLE Pipeline (
    PipelineID SERIAL PRIMARY KEY,
    PipelineName VARCHAR(255) NOT NULL,
    Description TEXT,                              -- Description of the pipeline’s purpose
    PipelineScheduleCron VARCHAR(50),              -- Cron expression for schedule
    PipelineGroup VARCHAR(100),                    -- Grouping information for task grouping in the DAG
    TemplateID INT NOT NULL REFERENCES PipelineTemplate(TemplateID), -- Reference to pipeline template
    RetryCount INT DEFAULT 3,                      -- Retry count for tasks in the pipeline (overrides template default)
    RetryDelay INTERVAL DEFAULT INTERVAL '5 minutes', -- Retry delay for tasks in the pipeline (overrides template default)
    DependentPipelineID INT REFERENCES Pipeline(PipelineID), -- Defines dependencies between pipelines
    ExecutionWindow INTERVAL,                      -- Defines a time window for running the pipeline
    AlertEmail VARCHAR(255),                       -- Email for notifications
    Owner VARCHAR(255),                            -- Owner of the pipeline
    LastRunStatus VARCHAR(50),                     -- Status of the last run (e.g., "Success", "Failed")
    LastRunTimestamp TIMESTAMP,                    -- Timestamp of the last run
    MaxParallelTasks INT DEFAULT 5,                -- Limits maximum concurrent tasks within this pipeline
    PipelineParameters JSONB,                      -- JSON formatted parameters for pipeline-level configurations
    Priority INT DEFAULT 1                         -- Pipeline execution priority
);



=============================================================

CREATE TABLE PipelineDatasetMap (
    PipelineDatasetMapID SERIAL PRIMARY KEY,
    PipelineID INT NOT NULL REFERENCES Pipeline(PipelineID), -- Links to the pipeline (DAG)
    DatasetID INT NOT NULL REFERENCES Dataset(DatasetID),    -- Links to the specific dataset
    DatasetEntityType VARCHAR(100),                          -- Defines the entity type (e.g., source, target)
    SliceID INT REFERENCES Slice(SliceID),                   -- Defines a specific data slice for partitioning tasks
    TransformationType VARCHAR(100),                         -- Type of transformation (e.g., join, aggregate)
    IsMandatory BOOLEAN DEFAULT TRUE,                        -- Flag to indicate if dataset is required for the pipeline
    DependencyLevel INT DEFAULT 1,                           -- Level of dependency, higher value runs first
    MappingParameters JSONB,                                 -- JSON formatted parameters for dataset-pipeline mapping
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP          -- Timestamp of the last modification
);
