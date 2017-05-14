USE [loggingDBCC]
GO

/****** Object:  Table [dbo].[dbccLogDetail]    Script Date: 5/14/2017 11:49:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[dbccLogDetail](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[masterID] [int] NOT NULL,
	[databaseName] [nvarchar](255) NOT NULL,
	[filePath] [nvarchar](255) NOT NULL,
	[errors] [smallint] NOT NULL,
	[errMsg] [nvarchar](4000) NOT NULL,
	[startTime] [datetime] NOT NULL,
	[endTime] [datetime] NULL,
 CONSTRAINT [PK_loggingDetail] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

/****** Object:  Table [dbo].[dbccLogMaster]    Script Date: 5/14/2017 11:49:29 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[dbccLogMaster](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](255) NOT NULL,
	[checkedDBs] [int] NOT NULL,
	[errors] [int] NOT NULL,
	[startTime] [datetime] NOT NULL,
	[endTime] [datetime] NULL,
 CONSTRAINT [PK_loggingMaster] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[dbccLogDetail] ADD  CONSTRAINT [DF_loggingDetail_startTime]  DEFAULT (getdate()) FOR [startTime]
GO

ALTER TABLE [dbo].[dbccLogMaster] ADD  CONSTRAINT [DF_dbccLogMaster_checkedDBs]  DEFAULT ((0)) FOR [checkedDBs]
GO

ALTER TABLE [dbo].[dbccLogMaster] ADD  CONSTRAINT [DF_loggingMaster_startTime]  DEFAULT (getdate()) FOR [startTime]
GO

ALTER TABLE [dbo].[dbccLogDetail]  WITH CHECK ADD  CONSTRAINT [FK_loggingDetail_masterID] FOREIGN KEY([masterID])
REFERENCES [dbo].[dbccLogMaster] ([id])
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[dbccLogDetail] CHECK CONSTRAINT [FK_loggingDetail_masterID]
GO


