CREATE TABLE Consumer (
    ConsumerID SERIAL PRIMARY KEY,
    ConsumerName VARCHAR(255),
    LOBName VARCHAR(255),
    ConsumerContact VARCHAR(255)
);

CREATE TABLE ConsumerAccounts (
    AccountID SERIAL PRIMARY KEY,
    AccountName VARCHAR(255),
    ConsumerID INT REFERENCES Consumer(ConsumerID)
);

CREATE TABLE ConsumerModelMap (
    ConsumerID INT REFERENCES Consumer(ConsumerID),
    ModelID INT REFERENCES Model(ModelID),
    PRIMARY KEY (ConsumerID, ModelID)
);

CREATE TABLE DataSources (
    SourceSystemID SERIAL PRIMARY KEY,
    SourceSystemName VARCHAR(255),
    SourceContact VARCHAR(255)
);

CREATE TABLE FileAttendance (
    AttendanceID SERIAL PRIMARY KEY,
    FileReceivedTime TIMESTAMP,
    ProcessStartTime TIMESTAMP,
    ProcessEndTime TIMESTAMP
);

CREATE TABLE FeedFiles (
    SliceID INT PRIMARY KEY,
    SourceSystemID INT REFERENCES DataSources(SourceSystemID),
    FeedGroup VARCHAR(255),
    ActiveIndicator BOOLEAN,
    FileName VARCHAR(255),
    ArchiveDirectory VARCHAR(255),
    SLAStartTime TIMESTAMP,
    SLAEndTime TIMESTAMP,
    FileReceivedTime TIMESTAMP,
    FileDirectory VARCHAR(255),
    ReportDatePath VARCHAR(255),
    FileRetentionPeriod INT,
    TrailerRowIndicator VARCHAR(255),
    TrailerRecordCount INT
);

CREATE TABLE Model (
    ModelID SERIAL PRIMARY KEY,
    ModelType VARCHAR(255),
    ModelDescription TEXT,
    ModelCatalogueReference VARCHAR(255)
);

CREATE TABLE ModelMap (
    LogicalModelID INT,
    PhysicalModelID INT,
    ActiveIndicator BOOLEAN,
    PRIMARY KEY (LogicalModelID, PhysicalModelID)
);

CREATE TABLE Dataset (
    DatasetID SERIAL PRIMARY KEY,
    DatasetName VARCHAR(255),
    PhysicalModelName VARCHAR(255),
    ModelID INT REFERENCES Model(ModelID)
);

CREATE TABLE TableDatasets (
    DatasetID INT REFERENCES Dataset(DatasetID),
    TableName VARCHAR(255),
    TableType VARCHAR(50),
    RetentionPeriod INT,
    PRIMARY KEY (DatasetID, TableName)
);

CREATE TABLE Calendar (
    CalendarID SERIAL PRIMARY KEY,
    CalendarName VARCHAR(255),
    HolidaySchedule BOOLEAN
);

CREATE TABLE DataProducts (
    DataProductID SERIAL PRIMARY KEY,
    DataProductName VARCHAR(255),
    DataSubProduct VARCHAR(255)
);

CREATE TABLE Slice (
    SliceID SERIAL PRIMARY KEY,
    DataProductID INT REFERENCES DataProducts(DataProductID),
    SliceName VARCHAR(255),
    Region VARCHAR(50),
    Description TEXT
);

CREATE TABLE ProcessAudit (
    LEventID SERIAL PRIMARY KEY,
    ParentLEVID INT,
    ConsumerID INT REFERENCES Consumer(ConsumerID),
    AccountID INT REFERENCES ConsumerAccounts(AccountID),
    ExecutionStatus VARCHAR(50),
    ExecutionStartTime TIMESTAMP,
    ExecutionEndTime TIMESTAMP,
    ExecutionDate TIMESTAMP,
    ExecutionUser VARCHAR(255),
    ExecutionID VARCHAR(255),
    ProcessName VARCHAR(255),
    ProcessOwner VARCHAR(255)
);


-- Insert records into Consumer
INSERT INTO Consumer (ConsumerName, LOBName, ConsumerContact)
VALUES
('John Doe', 'Retail', 'john.doe@example.com'),
('Jane Smith', 'Finance', 'jane.smith@example.com'),
('Alice Johnson', 'Technology', 'alice.johnson@example.com');

-- Insert records into ConsumerAccounts
INSERT INTO ConsumerAccounts (AccountName, ConsumerID)
VALUES
('Primary Account', 1),
('Savings Account', 2),
('Investment Account', 3);

-- Insert records into Model
INSERT INTO Model (ModelType, ModelDescription, ModelCatalogueReference)
VALUES
('Credit Model', 'Model for credit scoring', 'CAT-123'),
('Risk Model', 'Model for risk assessment', 'CAT-456'),
('Fraud Model', 'Model for fraud detection', 'CAT-789');

-- Insert records into ConsumerModelMap
INSERT INTO ConsumerModelMap (ConsumerID, ModelID)
VALUES
(1, 1),
(2, 2),
(3, 3);

-- Insert records into DataSources
INSERT INTO DataSources (SourceSystemName, SourceContact)
VALUES
('System A', 'contactA@example.com'),
('System B', 'contactB@example.com'),
('System C', 'contactC@example.com');

-- Insert records into FeedFiles
INSERT INTO FeedFiles (
    SliceID, SourceSystemID, FeedGroup, ActiveIndicator, FileName, ArchiveDirectory, SLAStartTime, SLAEndTime,
    FileReceivedTime, FileDirectory, ReportDatePath, FileRetentionPeriod, TrailerRowIndicator, TrailerRecordCount
)
VALUES
(1, 1, 'Group A', TRUE, 'file1.csv', '/archive/path1', '2024-11-15 08:00:00', '2024-11-15 17:00:00',
 '2024-11-15 09:00:00', '/data/path1', '/report/path1', 30, 'YES', 100),
(2, 2, 'Group B', TRUE, 'file2.csv', '/archive/path2', '2024-11-16 08:00:00', '2024-11-16 17:00:00',
 '2024-11-16 09:00:00', '/data/path2', '/report/path2', 45, 'NO', 150),
(3, 3, 'Group C', FALSE, 'file3.csv', '/archive/path3', '2024-11-17 08:00:00', '2024-11-17 17:00:00',
 '2024-11-17 09:00:00', '/data/path3', '/report/path3', 60, 'YES', 200);

-- Insert records into ModelMap
INSERT INTO ModelMap (LogicalModelID, PhysicalModelID, ActiveIndicator)
VALUES
(1, 1, TRUE),
(2, 2, TRUE),
(3, 3, FALSE);

-- Insert records into Dataset
INSERT INTO Dataset (DatasetName, PhysicalModelName, ModelID)
VALUES
('Customer Data', 'Physical Model A', 1),
('Transaction Data', 'Physical Model B', 2),
('Product Data', 'Physical Model C', 3);

-- Insert records into TableDatasets
INSERT INTO TableDatasets (DatasetID, TableName, TableType, RetentionPeriod)
VALUES
(1, 'CustomerTable', 'BASE', 365),
(2, 'TransactionTable', 'DELTA', 180),
(3, 'ProductTable', 'HISTORICAL', 730);

-- Insert records into Calendar
INSERT INTO Calendar (CalendarName, HolidaySchedule)
VALUES
('2024 Calendar', TRUE),
('2025 Calendar', FALSE),
('2026 Calendar', TRUE);

-- Insert records into DataProducts
INSERT INTO DataProducts (DataProductName, DataSubProduct)
VALUES
('Product A', 'SubProduct A1'),
('Product B', 'SubProduct B1'),
('Product C', 'SubProduct C1');

-- Insert records into Slice
INSERT INTO Slice (DataProductID, SliceName, Region, Description)
VALUES
(1, 'Slice A', 'North America', 'Description of Slice A'),
(2, 'Slice B', 'Europe', 'Description of Slice B'),
(3, 'Slice C', 'Asia', 'Description of Slice C');

-- Insert records into FileAttendance
INSERT INTO FileAttendance (FileReceivedTime, ProcessStartTime, ProcessEndTime)
VALUES
('2024-11-15 10:00:00', '2024-11-15 10:05:00', '2024-11-15 10:10:00'),
('2024-11-16 11:00:00', '2024-11-16 11:05:00', '2024-11-16 11:10:00'),
('2024-11-17 12:00:00', '2024-11-17 12:05:00', '2024-11-17 12:10:00');

-- Insert records into ProcessAudit
INSERT INTO ProcessAudit (
    ParentLEVID, ConsumerID, AccountID, ExecutionStatus, ExecutionStartTime, ExecutionEndTime,
    ExecutionDate, ExecutionUser, ExecutionID, ProcessName, ProcessOwner
)
VALUES
(1, 1, 1, 'Completed', '2024-11-15 11:00:00', '2024-11-15 11:30:00', '2024-11-15', 'admin', 'EXE-001', 'Data Ingestion', 'John Manager'),
(2, 2, 2, 'Failed', '2024-11-16 11:00:00', '2024-11-16 11:30:00', '2024-11-16', 'admin', 'EXE-002', 'Data Transformation', 'Jane Supervisor'),
(3, 3, 3, 'In Progress', '2024-11-17 11:00:00', NULL, '2024-11-17', 'admin', 'EXE-003', 'Data Loading', 'Alice Lead');
