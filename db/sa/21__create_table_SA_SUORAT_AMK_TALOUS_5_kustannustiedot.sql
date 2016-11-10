USE [VIPUNEN_SA]
GO
/****** Object:  Table [dbo].[SA_SUORAT_AMK_TALOUS_5_kustannustiedot]    Script Date: 10.11.2016 11:16:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SA_SUORAT_AMK_TALOUS_5_kustannustiedot]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[SA_SUORAT_AMK_TALOUS_5_kustannustiedot](
	[AMK] [nvarchar](50) NULL,
	[TILIK] [int] NULL,
	[KOULALA] [nvarchar](50) NULL,
	[AMK_koodi] [nvarchar](50) NULL,
	[OHJALA] [int] NULL,
	[VTKUSTY] [bigint] NULL,
	[KOULUTUS] [bigint] NULL,
	[TUTKOUL] [bigint] NULL,
	[PTUTOSAT] [bigint] NULL,
	[AMMOPKOUL] [bigint] NULL,
	[MUUKOUL] [bigint] NULL,
	[TUTKIMUS] [bigint] NULL,
	[YLAMKKOUL] [bigint] NULL,
	[TKTOIM] [bigint] NULL,
	[MUUYHTEI] [bigint] NULL,
	[CHECKSUM_AMK_TALOUS_5_KUSTANNUSTIEDOT] [int] NULL,
	[IMP_CREATED_DATE] [datetime] NULL,
	[IMP_UPDATED_DATE] [datetime] NULL,
	[IMP_CREATED_BY] [nvarchar](255) NULL,
	[IMP_DELETED_DATE] [datetime] NULL
) ON [PRIMARY]
END
GO