/*****************************************************************************
Copyright (c) 2017 SQL Workbooks LLC
Terms of Use: https://sqlworkbooks.com/terms-of-service/
Contact: help@sqlworkbooks.com

This script:
    Is only suitable for test environments
    Restores and modifies data in BabbyNames
    Creates and loads large tables

Note: this script assumes you have a backup of BabbyNames
    at 'S:\MSSQL\Backup\BabbyNames.bak'
    You need to either change that, or put a backup there :)
****************************************/


/****************************************************
Take a backup of this expanded database after running the setup script,
so you can just restore it in the future instead of re-running setup
****************************************************/

/*
--BACKUP DATABASE BabbyNames
-- TO DISK = N'S:\MSSQL\Backup\BabbyNames_3ProblemsPartitioning_1_of_4.bak',
--    DISK = N'S:\MSSQL\Backup\BabbyNames_3ProblemsPartitioning_2_of_4.bak',
--    DISK = N'S:\MSSQL\Backup\BabbyNames_3ProblemsPartitioning_3_of_4.bak',
--    DISK = N'S:\MSSQL\Backup\BabbyNames_3ProblemsPartitioning_4_of_4.bak'
-- WITH COMPRESSION, INIT;
--GO

use master;
GO
IF DB_ID('BabbyNames') IS NOT NULL
BEGIN
    ALTER DATABASE BabbyNames SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
END
GO
RESTORE DATABASE BabbyNames
    FROM DISK = N'S:\MSSQL\Backup\BabbyNames_3-Problems-Partitioning.bak' WITH REPLACE;
GO

*/


/****************************************************
Restore Small BabbyNames database with prejudice
****************************************************/

SET STATISTICS IO, TIME OFF;
GO
SET NOCOUNT ON;
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO


use master;
GO

IF DB_ID('BabbyNames') IS NOT NULL
BEGIN
	ALTER DATABASE BabbyNames
		SET SINGLE_USER
		WITH ROLLBACK IMMEDIATE;
END
GO

RESTORE DATABASE BabbyNames
	FROM DISK=N'S:\MSSQL\Backup\BabbyNames.bak'
	WITH
        MOVE 'BabbyNames' TO 'S:\MSSQL\Data\BabbyNames.mdf',
        MOVE 'BabbyNames_log' TO 'S:\MSSQL\Data\BabbyNames_log.ldf',
		REPLACE,
		RECOVERY;
GO

ALTER DATABASE BabbyNames SET RECOVERY SIMPLE;
GO


/* SQL Server 2014+ */
ALTER DATABASE BabbyNames SET COMPATIBILITY_LEVEL=130;
GO

/* SQL Server 2012+ */
ALTER DATABASE BabbyNames SET TARGET_RECOVERY_TIME = 60 SECONDS;
GO

USE BabbyNames;
GO

EXEC evt.logme N'Restored database. Let the games begin.';
GO

/* SQL Server 2016+ */
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = ON;
GO

ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO

exec sp_configure 'show advanced options', 1;
GO
RECONFIGURE
GO

exec sp_configure 'max degree of parallelism', 4;
GO

RECONFIGURE
GO


/* This script expands the data a couple of times. */
ALTER DATABASE BabbyNames MODIFY FILE (NAME='BabbyNames', SIZE=10GB);
GO

ALTER DATABASE BabbyNames MODIFY FILE (NAME='BabbyNames_log', SIZE=5GB);
GO


/******************************************************/
/* FILEGROUPS                                         */
/******************************************************/

EXEC evt.logme N'ADD FILEGROUP fg_FirstNameByBirthDate_pt;';
GO

ALTER DATABASE BabbyNames
    ADD FILEGROUP fg_FirstNameByBirthDate_pt;
GO

EXEC evt.logme N'ADD FILE fg_FirstNameByBirthDate_pt_1;';
GO

ALTER DATABASE BabbyNames 
ADD FILE 
(
    NAME = fg_FirstNameByBirthDate_pt_1,
    /* Change Location 1 of 2 */
    FILENAME = 'S:\MSSQL\Data\fg_FirstNameByBirthDate_pt_1.ndf',
    SIZE = 10GB,
    MAXSIZE = 20GB,
    FILEGROWTH = 512MB
) TO FILEGROUP fg_FirstNameByBirthDate_pt;
GO



ALTER DATABASE BabbyNames SET DELAYED_DURABILITY = FORCED;
GO

/******************************************************/
/* ref.Numbers                                        */
/******************************************************/

/* Create ref.Numbers. This is a helper "numbers" table just to help us in the next step.*/
IF SCHEMA_ID('ref') IS NULL
BEGIN
	EXEC evt.logme N'Create schema ref.';

	EXEC ('CREATE SCHEMA ref AUTHORIZATION dbo');
END
GO

EXEC evt.logme N'Create ref.Numbers.';
GO
IF OBJECT_ID('ref.Numbers','U') IS NOT NULL
BEGIN
	EXEC evt.logme N'Table ref.Numbers already exists, dropping.';

	DROP TABLE ref.Numbers;
END
GO

CREATE TABLE ref.Numbers (
	Num INT NOT NULL,
);
GO

EXEC evt.logme N'Load ref.Numbers.';
GO
INSERT ref.Numbers WITH (TABLOCK)
	(Num)
SELECT TOP 10000000
	ROW_NUMBER() OVER (ORDER BY fn1.ReportYear)
FROM agg.FirstNameByYear AS fn1
CROSS JOIN agg.FirstNameByYear AS fn2;
GO


EXEC evt.logme N'Key ref.Numbers.';
GO
ALTER TABLE ref.Numbers
	ADD CONSTRAINT pk_refNumbers_Num
		PRIMARY KEY CLUSTERED (Num);
GO




/******************************************************/
/* dbo.loaddetail                                        */
/******************************************************/

IF OBJECT_ID('dbo.loaddetail') IS NULL
	EXEC ('CREATE PROCEDURE dbo.loaddetail AS RETURN 0');
GO

ALTER PROCEDURE dbo.loaddetail
	@min INT  /* we've got data since 1880*/,
	@max INT,
    @schema NVARCHAR(3) = N'dbo'
AS
	DECLARE @msg NVARCHAR(512);
	DECLARE @tablename NVARCHAR(256);
	DECLARE @dsql NVARCHAR(MAX);

	SET @tablename = @schema + N'.FirstNameByBirthDate_'  + CAST(@min as NVARCHAR(4)) + '_' + CAST(@max as NVARCHAR(4));

	SET @msg = N'Creating table ' + @tablename + N'.';

	EXEC evt.logme @msg;

	IF OBJECT_ID(@tablename,'U') IS NOT NULL
	BEGIN
		SET @msg= N'Table ' + @tablename + N' already exists, dropping.';

		EXEC evt.logme @msg;

		SET @dsql= N'DROP TABLE ' + @tablename + N';'

		EXEC (@dsql);
	END

	SET @dsql = N'
	CREATE TABLE ' + @tablename + N' ( 
		FakeBirthDateStamp DATETIME2(0),
		FirstNameByBirthDateId BIGINT IDENTITY NOT NULL,
	    BirthYear AS YEAR(FakeBirthDateStamp) PERSISTED NOT NULL ,
		FirstNameId INT,
		Gender CHAR(1) NULL
	);'

	EXEC (@dsql);

	WHILE @min <= @max
	BEGIN

		SET @msg = N'Inserting data into ' + @tablename + ' for year ' + cast(@min as nvarchar(4)) + N'.';

		EXEC evt.logme @msg;


		SET @dsql=N'
		INSERT '+ @tablename + ' WITH (TABLOCK)
			(FakeBirthDateStamp, FirstNameId, Gender
			)
		SELECT
			DATEADD(mi,num.Num * 5.1,CAST(''1/1/'' + CAST(fn.ReportYear AS CHAR(4)) AS DATETIME2(0))) as FakeBirthDateStamp,
			fn.FirstNameId,
			fn.Gender
		FROM agg.FirstNameByYear AS fn
		RIGHT OUTER JOIN ref.Numbers num on
			num.Num <= fn.NameCount
		WHERE fn.ReportYear = ' +  CAST(@min AS NCHAR(4)) + N'
		ORDER BY FakeBirthDateStamp
			OPTION (RECOMPILE);'

		EXEC sp_executesql @dsql;

		SET @min=@min+1
	END

	SET @msg = N'Create clustered index on table ' + @tablename + N'.';

	EXEC evt.logme @msg;

    SET @dsql=N'
	CREATE UNIQUE CLUSTERED INDEX cx_FirstNameByBirthDate_' 
		+ CAST(@min as NVARCHAR(4)) + N'_' + CAST(@max as NVARCHAR(4)) + N'
		ON ' + @tablename + N' (FirstNameByBirthDateId, FakeBirthDateStamp)
		WITH (SORT_IN_TEMPDB = ON);'

	EXEC sp_executesql @dsql;
GO



/******************************************************/
/* Helper index                                       */
/******************************************************/

EXEC evt.logme N'Create ix_halp ON agg.FirstNameByYear.';
GO
CREATE NONCLUSTERED INDEX ix_halp
	ON agg.FirstNameByYear (ReportYear)
INCLUDE (FirstNameId, Gender)
WITH (SORT_IN_TEMPDB = ON);
GO



/******************************************************/
/* Turn on Query Store                                */
/******************************************************/
ALTER DATABASE [BabbyNames] SET QUERY_STORE = ON
GO
ALTER DATABASE [BabbyNames] SET QUERY_STORE (OPERATION_MODE = READ_WRITE)
GO

/******************************************************/
/* Load Data                                          */
/******************************************************/


EXEC evt.logme N'Load dbo.FirstNameByBirthDate_1976_2015.';
GO
EXEC dbo.loaddetail 1976, 2015, N'dbo';
GO


/******************************************************/
/* Create schema pt */
/******************************************************/

EXEC evt.logme N'CREATE SCHEMA pt AUTHORIZATION dbo';
GO
CREATE SCHEMA pt AUTHORIZATION dbo;
GO


EXEC evt.logme N'CREATE PARTITION FUNCTION pf_fnbd.';
GO
CREATE PARTITION FUNCTION pf_fnbd (DATETIME2(0))
    AS RANGE RIGHT
    FOR VALUES ( '1974-01-01', '1975-01-01',
        '1976-01-01', '1977-01-01', '1978-01-01', '1979-01-01', '1980-01-01',
        '1981-01-01', '1982-01-01', '1983-01-01', '1984-01-01', '1985-01-01',
        '1986-01-01', '1987-01-01', '1988-01-01', '1989-01-01', '1990-01-01',
        '1991-01-01', '1992-01-01', '1993-01-01', '1994-01-01', '1995-01-01',
        '1996-01-01', '1997-01-01', '1998-01-01', '1999-01-01', '2000-01-01',
        '2001-01-01', '2002-01-01', '2003-01-01', '2004-01-01', '2005-01-01',
        '2006-01-01', '2007-01-01', '2008-01-01', '2009-01-01', '2010-01-01',
        '2011-01-01', '2012-01-01', '2013-01-01', '2014-01-01', '2015-01-01',
        '2016-01-01', '2017-01-01', '2018-01-01', '2019-01-01', '2020-01-01'
        );
GO

EXEC evt.logme N'CREATE PARTITION SCHEME ps_fnbd';
GO
CREATE PARTITION SCHEME ps_fnbd
    AS PARTITION pf_fnbd
    ALL TO ([fg_FirstNameByBirthDate_pt]);
GO

EXEC evt.logme N'CREATE table pt.FirstNameByBirthDate_1976_2015';
GO
CREATE TABLE pt.FirstNameByBirthDate_1976_2015 (
	FakeBirthDateStamp DATETIME2(0) NOT NULL,
	FirstNameByBirthDateId BIGINT IDENTITY NOT NULL,
    BirthYear AS YEAR(FakeBirthDateStamp) PERSISTED NOT NULL ,
	FirstNameId INT NULL,
	Gender CHAR(1) NULL
) ON ps_fnbd (FakeBirthDateStamp) 
GO



EXEC evt.logme N'Insert data from FirstNameByBirthDate_1976_2015';
GO
SET IDENTITY_INSERT pt.FirstNameByBirthDate_1976_2015 ON;  
GO 

INSERT pt.FirstNameByBirthDate_1976_2015 WITH (TABLOCK)
    (FirstNameByBirthDateId, FakeBirthDateStamp, FirstNameId, Gender)  
SELECT FirstNameByBirthDateId, FakeBirthDateStamp, FirstNameId, Gender
FROM dbo.FirstNameByBirthDate_1976_2015
    OPTION (QUERYTRACEON 610)
GO

SET IDENTITY_INSERT pt.FirstNameByBirthDate_1976_2015 OFF;  
GO 



EXEC evt.logme N'CREATE UNIQUE CLUSTERED INDEX cx_FirstNameByBirthDate_pt';
GO
CREATE UNIQUE CLUSTERED INDEX cx_pt_FirstNameByBirthDate_1976_2015 ON
    pt.FirstNameByBirthDate_1976_2015 (FirstNameByBirthDateId, FakeBirthDateStamp)
	WITH (SORT_IN_TEMPDB = ON);
GO



/******************************************************/
/* Cleanup                                            */
/******************************************************/

EXEC evt.logme N'Clean up ix_halp ON agg.FirstNameByYear.';
GO
DROP INDEX ix_halp ON agg.FirstNameByYear;
GO


/******************************************************/
/* Create indexes for demos                           */
/******************************************************/

/* nonclustered rowstore... */
EXEC evt.logme N'Create index ix_dbo_FirstNameByBirthDate_1976_2015_BirthYear.';
GO
CREATE INDEX ix_dbo_FirstNameByBirthDate_1976_2015_BirthYear
	on dbo.FirstNameByBirthDate_1976_2015 (BirthYear)
	WITH (SORT_IN_TEMPDB = ON);
GO

EXEC evt.logme N'Create index ix_pt_FirstNameByBirthDate_1976_2015_BirthYear.';
GO
CREATE INDEX ix_pt_FirstNameByBirthDate_1976_2015_BirthYear
	on pt.FirstNameByBirthDate_1976_2015 (BirthYear)
	WITH (SORT_IN_TEMPDB = ON);
GO


/* nonclustered columnstore... */
EXEC evt.logme N'Create index col_pt_FirstNameByBirthDate_1976_2015.';
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX col_pt_FirstNameByBirthDate_1976_2015
	on pt.FirstNameByBirthDate_1976_2015 
	( FakeBirthDateStamp, FirstNameByBirthDateId, FirstNameId, Gender);
GO

EXEC evt.logme N'Create index col_dbo_FirstNameByBirthDate_1976_2015.';
GO
CREATE NONCLUSTERED COLUMNSTORE INDEX col_dbo_FirstNameByBirthDate_1976_2015
	on dbo.FirstNameByBirthDate_1976_2015 
	( FakeBirthDateStamp, FirstNameByBirthDateId, FirstNameId, Gender);
GO

ALTER DATABASE BabbyNames SET DELAYED_DURABILITY = DISABLED;
GO 
