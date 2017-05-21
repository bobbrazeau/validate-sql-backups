USE [loggingDBCC]
GO

/****** Object:  StoredProcedure [dbo].[dbccLogDB]    Script Date: 5/14/2017 3:28:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Bob Brazeau
-- Create date: 4/15/2017
-- Description:	save check of each database  
-- =============================================
CREATE PROCEDURE [dbo].[dbccLogDB]
	@masterID int,
	@dbName nvarchar(255),
	@filePath nvarchar(255),
	@errors int,
	@errorMsg nvarchar(4000),
	@startTime datetime,
	@endTime datetime
AS
BEGIN

	insert into [dbo].dbcclogDetail (masterID, databaseName, filePath, errors, errMsg, startTime, endTime)
	values(@masterID, @dbName, @filePath, @errors, @errorMsg, @startTime, @endTime);
END


GO

/****** Object:  StoredProcedure [dbo].[dbccLogEnd]    Script Date: 5/14/2017 3:28:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Bob Brazeau
-- Create date: 4/15/2017
-- Description:	finalize server record summing db's with errors  
-- =============================================
CREATE PROCEDURE [dbo].[dbccLogEnd]
	@id int
AS
BEGIN
	declare @count int = 0, @checked int = 0;

	select @count = isnull(sum(errors),0), @checked = isnull(count(*),0)	
	from [dbo].dbccLogDetail 
	where masterID = @id;

	update [dbo].[dbccLogMaster]
	set errors = @count, endTime = getdate(), checkedDBs = @checked
	where id = @id;
END


GO

/****** Object:  StoredProcedure [dbo].[dbccLogStart]    Script Date: 5/14/2017 3:28:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Bob Brazeau
-- Create date: 4/15/2017
-- Description:	verifying restores of databases using DBCC checkdb.  
-- Initalize record at server level, endTime is set when finished
-- =============================================
CREATE PROCEDURE [dbo].[dbccLogStart]
	@serverName nvarchar(255)
	,@id int output
AS
BEGIN

	insert into [dbo].dbccLogMaster (serverName, errors, startTime)
	values(@serverName, 0, getdate());

	set @id =  SCOPE_IDENTITY() -- return ID just added.  @@Identity can get tripped up by triggers.
	RETURN
END


GO
