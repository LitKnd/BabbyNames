/****************************************
Copyright 2017 Kendra Little

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

https://opensource.org/licenses/MIT
****************************************/

/*************************************
Notes: 
    This script has only been tested against SQL Server 2016 Developer Edition.
    SQL Server 2016 Dev Edition is free with Visual Studio Dev Essentials (which is free)
    https://blogs.technet.microsoft.com/dataplatforminsider/2016/03/31/microsoft-sql-server-developer-edition-is-now-free/

Instructions:

    Download names.zip
        names.zip is originally from https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-level-data
        The original file is licensed under cc-zero (public domain dedication)

    Extract all files from names.zip into C:\BabbyNamesImport

    Modify the DataFilePath and LogFilePath in the script below to a location where you want your data and log files

    Review the script and make sure the settings make sense for your instance

    Enable SQLCMD in this SSMS Session (Query Menu, SQLCMD mode)

    Run the script to import tables and create procedures.

    Uncomment the backup command at the end and run that command alone to create compressed backup for distribution.
*************************************/

/* Don't remove the trailing \ or it will fail. */
:SETVAR DataSourcePath "C:\BabbyNamesImport\"

/* Database data file path - requires 5GB */
:SETVAR DataFilePath "S:\MSSQL\Data\"

/* Database log file path - requires 1GB, can be the same as data file path */
:SETVAR LogFilePath "S:\MSSQL\Data\"


/****************************************
SQL Server Settings 
****************************************/
exec sp_configure 'show advanced options', 1;
GO
RECONFIGURE
GO

exec sp_configure 'max server memory (MB)', 3500;
GO
RECONFIGURE
GO

/* 
We're using defaults here for parallelism settings
These ain't always the best for production
But they're where we're starting for learning */
exec sp_configure 'cost threshold for parallelism', 5;
GO
exec sp_configure 'max degree of parallelism', 0;
GO

RECONFIGURE
GO

/****************************************
SET stuff n things
****************************************/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
SET STATISTICS TIME, IO OFF;
GO
SET NOCOUNT ON;
GO

:SETVAR Versn "1_200"

/****************************************
Recreate BabbyNames database.
****************************************/
DECLARE @msg NVARCHAR(MAX)
SET @msg=N'Drop BabbyNames database if it exists.'

DECLARE	@msgtime NVARCHAR(24)
SET @msgtime = CONVERT(NVARCHAR(21), SYSDATETIME(), 121) +  N': ';

SET @msg=@msgtime+@msg;
RAISERROR (@msg, 0, 1) WITH NOWAIT;
GO

USE master;
GO


IF DB_ID('BabbyNames') IS NOT NULL
BEGIN
	ALTER DATABASE BabbyNames SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE BabbyNames;
END
GO

DECLARE @msg NVARCHAR(MAX)
SET @msg = N'Create BabbyNames database.';

DECLARE	@msgtime NVARCHAR(24) 
SET @msgtime =  CONVERT(NVARCHAR(21), SYSDATETIME(), 121) +  N': ';
SET @msg=@msgtime+@msg;
RAISERROR (@msg, 0, 1) WITH NOWAIT;
GO

CREATE DATABASE BabbyNames
ON PRIMARY
	(NAME = BabbyNames, FILENAME =  N'$(DataFilePath)BabbyNames.mdf', 
	SIZE = 1GB, FILEGROWTH = 256MB, MAXSIZE=50GB)
LOG ON
	(NAME = BabbyNames_log, FILENAME = N'$(LogFilePath)BabbyNames_log.ldf', 
	SIZE = 512MB, FILEGROWTH = 256MB, MAXSIZE=20GB)
GO


/****************************************
Use BabbyNames and create logging proc and table.
The logging proc contains the version info. Because I'm lazy.
****************************************/
DECLARE @msg NVARCHAR(MAX);
SET @msg= N'USE BabbyNames and create evt schema.';

DECLARE	@msgtime NVARCHAR(24);
SET @msgtime = CONVERT(NVARCHAR(21), SYSDATETIME(), 121) +  N': ';
SET @msg=@msgtime+@msg;
RAISERROR (@msg, 0, 1) WITH NOWAIT;
GO

USE BabbyNames;
GO

CREATE SCHEMA evt AUTHORIZATION dbo;
GO

IF OBJECT_ID('evt.logme') IS NULL
	EXEC ('CREATE PROCEDURE evt.logme as RETURN 0;')
GO

ALTER PROCEDURE evt.logme
	@msg NVARCHAR(MAX)
AS
	SET NOCOUNT ON;

	DECLARE @ver NVARCHAR(128);
	SET @ver = N'(ver $(Versn)) ';

	SET @msg=@ver+@msg;

	DECLARE @msgtime NVARCHAR(24);
	SET @msgtime = CONVERT(NVARCHAR(21), SYSDATETIME(), 121) +  N': ';

	IF OBJECT_ID ('evt.Log') IS NOT NULL
		INSERT evt.Log (LogEntry) VALUES (@msg);

	SET @msg=@msgtime+@msg;
	RAISERROR (@msg, 0, 1) WITH NOWAIT;
GO

EXEC evt.logme N'Create evt.Log table.';
GO

CREATE TABLE evt.[Log] (
	LogId INT IDENTITY NOT NULL,
	LogDate DATETIME2(0) NOT NULL
		CONSTRAINT df_LogDate_sysdatetime 
		DEFAULT SYSDATETIME(),
	LogEntry NVARCHAR(MAX),
);
GO

ALTER TABLE evt.[Log]  
	ADD CONSTRAINT pk_evtLog
	PRIMARY KEY CLUSTERED (LogId)
	WITH (MAXDOP=1);
GO

/****************************************
Load the national src tables from disk using BULK INSERT 
****************************************/

EXEC evt.logme N'Load the national src tables from disk using BULK INSERT.';
GO


CREATE SCHEMA src AUTHORIZATION dbo;
GO

declare @min int
set @min = 1880;
declare @max int
set @max = 2015;
declare @tablename nvarchar(256);
declare @dsql nvarchar(max);

WHILE @min <= @max
BEGIN
	SET @tablename=N'names_' + cast (@min as NCHAR(4))

	EXEC evt.logme @tablename;

	IF (SELECT COUNT(*) from sys.objects where name=@tablename) = 1
	BEGIN
		SET @dsql=N'
		DROP TABLE src.' + @tablename + N';';
	
		EXEC(@dsql);
	END
	BEGIN
		SET @dsql=N'
		CREATE TABLE src.' + @tablename + N' (
			FirstName varchar(255) NOT NULL,
			Gender char(1) NOT NULL,
			NameCount INT NOT NULL
		);
		BULK INSERT BabbyNames.src.' + @tablename + N' 
			FROM ''$(DataSourcePath)yob' + cast (@min as char(4)) + N'.txt'' 
			WITH ( FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'');

		ALTER TABLE src.' + @tablename + N' 
			ADD ReportYear INT NOT NULL
				DEFAULT(' + cast (@min as char(4)) + N');
			
		ALTER TABLE src.' + @tablename + N' 
			ADD CONSTRAINT pk_src_' + @tablename + N' PRIMARY KEY CLUSTERED (ReportYear, FirstName, Gender);

		ALTER TABLE src.' + @tablename + N' WITH CHECK
			ADD CONSTRAINT ck_src_' + @tablename + N' CHECK (ReportYear = ' + cast (@min as char(4)) + N')
			';
				
		EXEC(@dsql);
	END
	SET @min=@min+1;
END
GO



/****************************************
Create a partitioned view for national src tables. 
****************************************/
EXEC evt.logme N'Create the partitioned view for national src tables.';

IF (SELECT COUNT(*) from sys.objects where name='names_all') = 1
	DROP VIEW src.names_all ;
GO

DECLARE @min INT;
SET @min=1881;
DECLARE @max INT;
SET @max=2015;
DECLARE @tablename NVARCHAR(256);
DECLARE @dsql NVARCHAR(MAX);

SET @dsql = N'
	CREATE VIEW src.names_all 
	AS 
		SELECT * FROM src.names_1880' + CHAR(10);

WHILE @min <= @max
BEGIN
	SET @tablename=N'src.names_' + cast (@min as char(4))

	SET @dsql=@dsql+
'		UNION ALL ' + CHAR(10) + 
'		SELECT * FROM ' + @tablename;

	SET @min=@min+1;
END

EXEC (@dsql);
GO



/****************************************
Create and load ref.FirstName and agg.FirstNameByYear
****************************************/

EXEC evt.logme N'Create schema ref.';
GO

CREATE SCHEMA ref AUTHORIZATION dbo;
GO

EXEC evt.logme N'Create ref.FirstName.';
GO

/* Not persisting NameLength just for demo opportunities later */
CREATE TABLE ref.FirstName(
	FirstNameId INT IDENTITY NOT NULL,
	FirstName VARCHAR(255) NOT NULL,
	NameLength AS LEN(FirstName),
	FirstReportYear INT NOT NULL,
	LastReportYear INT NOT NULL,
	TotalNameCount BIGINT NOT NULL
);
GO

EXEC evt.logme N'Key ref.FirstName.';
GO

ALTER TABLE ref.FirstName 
	ADD CONSTRAINT pk_FirstName_FirstNameId
	PRIMARY KEY CLUSTERED (FirstNameId);
GO

EXEC evt.logme N'Load ref.FirstName.';
GO

INSERT ref.FirstName WITH (TABLOCK)
	(FirstName,
	FirstReportYear,
	LastReportYear,
	TotalNameCount)
SELECT  
	FirstName,
	MIN(ReportYear) as FirstReportYear,
	MAX(ReportYear) as LastReportYear,
	SUM(NameCount) as TotalNameCount
FROM src.names_all
GROUP BY FirstName
OPTION (QUERYTRACEON 610);
GO

EXEC evt.logme N'Create schema agg.';
GO

CREATE SCHEMA agg AUTHORIZATION dbo;
GO

EXEC evt.logme N'Create agg.FirstNameByYear';
GO

CREATE TABLE agg.FirstNameByYear (
	ReportYear INT NOT NULL,
	FirstNameId INT NOT NULL,
	Gender char(1) NOT NULL,
	NameCount INT NOT NULL
);
GO

EXEC evt.logme N'Load agg.FirstNameByYear';
GO

INSERT agg.FirstNameByYear  WITH (TABLOCK)
	(ReportYear, FirstNameId, Gender, NameCount)
SELECT 
	na.ReportYear, 
	fn.FirstNameId, 
	na.Gender,
	na.NameCount
FROM src.names_all na
JOIN ref.FirstName fn on na.FirstName=fn.FirstName;
GO

EXEC evt.logme N'Key agg.FirstNameByYear';
GO

ALTER TABLE agg.FirstNameByYear  
	ADD CONSTRAINT pk_aggFirstNameByYear
	PRIMARY KEY CLUSTERED (ReportYear, FirstNameId, Gender)
	WITH (MAXDOP=1);
GO

EXEC evt.logme N'Create foreign key on agg.FirstNameByYear referencing ref.FirstName';
GO

ALTER TABLE agg.FirstNameByYear 
	ADD CONSTRAINT fk_FirstNameByYear_FirstName
	FOREIGN KEY (FirstNameId)
	REFERENCES ref.FirstName(FirstNameId);
GO


/****************************************
Drop national src objects
****************************************/

EXEC evt.logme N'Drop national src objects.';
GO

IF (SELECT COUNT(*) from sys.objects where name='names_all') = 1
	DROP VIEW src.names_all ;
GO

DECLARE @min INT;
SET @min=1880;
DECLARE @max INT;
SET @max=2015;
DECLARE @tablename NVARCHAR(256);
DECLARE @dsql NVARCHAR(MAX);

WHILE @min <= @max
BEGIN

	SET @tablename = N'src.names_' + cast (@min as nchar(4))
	SET @dsql = N'DROP TABLE ' + @tablename
	EXEC evt.logme @dsql;

	IF (SELECT OBJECT_ID(@tablename)) IS NOT NULL
	BEGIN
		EXEC(@dsql);
	END
	ELSE 
		EXEC evt.logme N'Table doesn''t exist, skipping.';

	SET @min=@min+1;
END
GO

/****************************************
Create and populate ref.State 
****************************************/

EXEC evt.logme N'Create and populate ref.State.';
GO

CREATE TABLE ref.State (
	StateCode CHAR(2) NOT NULL,
	StateName VARCHAR(128) NOT NULL,
    CONSTRAINT pk_ref_State PRIMARY KEY CLUSTERED (StateCode)
);
GO

INSERT ref.State (StateCode, StateName)
VALUES
    ('AL', 'Alabama'),
    ('AK', 'Alaska'),
    ('AZ', 'Arizona'),
    ('AR', 'Arkansas'),
    ('CA', 'California'),
    ('CO', 'Colorado'),
    ('CT', 'Connecticut'),
    ('DE', 'Delaware'),
    ('DC', 'District of Columbia'),
    ('FL', 'Florida'),
    ('GA', 'Georgia'),
    ('HI', 'Hawaii'),
    ('ID', 'Idaho'),
    ('IL', 'Illinois'),
    ('IN', 'Indiana'),
    ('IA', 'Iowa'),
    ('KS', 'Kansas'),
    ('KY', 'Kentucky'),
    ('LA', 'Louisiana'),
    ('ME', 'Maine'),
    ('MD', 'Maryland'),
    ('MA', 'Massachusetts'),
    ('MI', 'Michigan'),
    ('MN', 'Minnesota'),
    ('MS', 'Mississippi'),
    ('MO', 'Missouri'),
    ('MT', 'Montana'),
    ('NE', 'Nebraska'),
    ('NV', 'Nevada'),
    ('NH', 'New Hampshire'),
    ('NJ', 'New Jersey'),
    ('NM', 'New Mexico'),
    ('NY', 'New York'),
    ('NC', 'North Carolina'),
    ('ND', 'North Dakota'),
    ('OH', 'Ohio'),
    ('OK', 'Oklahoma'),
    ('OR', 'Oregon'),
    ('PA', 'Pennsylvania'),
    ('RI', 'Rhode Island'),
    ('SC', 'South Carolina'),
    ('SD', 'South Dakota'),
    ('TN', 'Tennessee'),
    ('TX', 'Texas'),
    ('UT', 'Utah'),
    ('VT', 'Vermont'),
    ('VA', 'Virginia'),
    ('WA', 'Washington'),
    ('WV', 'West Virginia'),
    ('WI', 'Wisconsin'),
    ('WY', 'Wyoming')


/****************************************
Load src.StateDataRaw from disk 
****************************************/
EXEC evt.logme N'Load src.StateDataRaw from disk.';
GO

CREATE TABLE src.StateDataRaw (
    StateCode CHAR(2) NOT NULL,
    Gender CHAR(1) NOT NULL,
    BirthYear INT NOT NULL,
    FirstName nvarchar(15) NOT NULL,
    NameCount int NOT NULL
);

declare @tablename nvarchar(256),
    @dsql nvarchar(max),
    @statecode char(2),
    @statename varchar(128);
 
DECLARE @StateDataLoad as CURSOR;
 
SET @StateDataLoad = CURSOR FOR
SELECT StateCode, StateName
 FROM ref.State;
 
OPEN @StateDataLoad;
FETCH NEXT FROM @StateDataLoad INTO @statecode, @statename;
 
WHILE @@FETCH_STATUS = 0
BEGIN

	EXEC evt.logme @statecode;

	BEGIN
		SET @dsql=N'
		BULK INSERT BabbyNames.src.StateDataRaw 
			FROM ''$(DataSourcePath)' + @statecode + N'.TXT'' 
			WITH ( FIELDTERMINATOR = '','', ROWTERMINATOR = ''\n'');			
			';
				
		EXEC(@dsql);
	END

 FETCH NEXT FROM @StateDataLoad INTO @statecode, @statename;
END
 
CLOSE @StateDataLoad;
DEALLOCATE @StateDataLoad;

ALTER TABLE src.StateDataRaw
ADD CONSTRAINT pk_src_StateDataRaw PRIMARY KEY CLUSTERED 
(StateCode, BirthYear, FirstName, Gender);
GO


/****************************************
Create and populate agg.FirstNameByYearState
****************************************/
EXEC evt.logme N'Create and populate agg.FirstNameByYearState';
GO

CREATE TABLE agg.FirstNameByYearState (
	ReportYear INT NOT NULL,
    StateCode char(2) NOT NULL,
	FirstNameId INT NOT NULL,
	Gender char(1) NOT NULL,
	NameCount INT NOT NULL
);
GO

INSERT agg.FirstNameByYearState  WITH (TABLOCK)
	(ReportYear, StateCode, FirstNameId, Gender, NameCount)
SELECT 
	na.BirthYear, 
    na.StateCode,
	fn.FirstNameId, 
	na.Gender,
	na.NameCount
FROM src.StateDataRaw na
JOIN ref.FirstName fn on na.FirstName=fn.FirstName;
GO

EXEC evt.logme N'Key agg.FirstNameByYearState';
GO

ALTER TABLE agg.FirstNameByYearState
	ADD CONSTRAINT pk_aggFirstNameByYearState
	PRIMARY KEY CLUSTERED (ReportYear, StateCode, FirstNameId, Gender);
GO

EXEC evt.logme N'Create foreign key on agg.FirstNameByYearState referencing ref.FirstName';
GO

ALTER TABLE agg.FirstNameByYearState
	ADD CONSTRAINT fk_FirstNameByYearState_FirstName
	FOREIGN KEY (FirstNameId)
	REFERENCES ref.FirstName(FirstNameId);
GO

DROP TABLE src.StateDataRaw;
GO

/****************************************
Report back on index sizes & finish up
****************************************/


TRUNCATE TABLE evt.Log;
GO

CHECKPOINT
GO

SELECT 
	sc.name + '.' + so.name as table_name,
	ps.index_id as index_id,
	ps.reserved_page_count * 8. /1024. as size_MB,
	ps.row_count
FROM sys.dm_db_partition_stats ps
JOIN sys.objects so on ps.object_id=so.object_id
JOIN sys.schemas sc on so.schema_id=sc.schema_id
WHERE so.is_ms_shipped=0
ORDER BY size_MB desc;
GO

EXEC evt.logme N'BabbyNames all done with data from 1881 to 2015.';
GO


--BACKUP DATABASE BabbyNames TO DISK = N'S:\MSSQL\Backup\BabbyNames.bak'
--	WITH COMPRESSION, FORMAT, INIT;
--GO

