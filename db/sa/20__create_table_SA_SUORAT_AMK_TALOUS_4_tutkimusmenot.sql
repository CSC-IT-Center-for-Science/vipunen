USE [VIPUNEN_SA]
GO
/****** Object:  Table [dbo].[SA_SUORAT_AMK_TALOUS_4_tutkimusmenot]    Script Date: 10.11.2016 11:16:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SA_SUORAT_AMK_TALOUS_4_tutkimusmenot]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[SA_SUORAT_AMK_TALOUS_4_tutkimusmenot](
	[TILIK] [int] NULL,
	[OPM95OPA] [nvarchar](50) NULL,
	[TUTMENOT] [bigint] NULL,
	[ULKOPTR] [bigint] NULL,
	[AKATEMIA] [bigint] NULL,
	[MUUOKM] [bigint] NULL,
	[TEKES] [bigint] NULL,
	[MUUTEM] [bigint] NULL,
	[ULKOMIN] [bigint] NULL,
	[OIKMIN] [bigint] NULL,
	[SISÄMIN] [bigint] NULL,
	[PUOLMIN] [bigint] NULL,
	[VALTVMIN] [bigint] NULL,
	[MMM] [bigint] NULL,
	[LVM] [bigint] NULL,
	[STM] [bigint] NULL,
	[YMPMIN] [bigint] NULL,
	[KUNNAT] [bigint] NULL,
	[MUUJULKR] [bigint] NULL,
	[KOTIRAHA] [bigint] NULL,
	[KOTIYRIT] [bigint] NULL,
	[ULKOYRIT] [bigint] NULL,
	[EUPUITER] [bigint] NULL,
	[EAKR] [bigint] NULL,
	[ESR] [bigint] NULL,
	[MUUEUR] [bigint] NULL,
	[ULKORAHA] [bigint] NULL,
	[KVJARJ] [bigint] NULL,
	[ULKOMUUR] [bigint] NULL,
	[OPM95OPA_koodi] [nvarchar](50) NULL,
	[AMK] [nvarchar](50) NULL,
	[PERUSR] [bigint] NULL,
	[AMKOMARAH] [bigint] NULL,
	[AMK_koodi] [nvarchar](50) NULL,
	[CHECKSUM_AMK_TALOUS_4_TUTKIMUSMENOT] [int] NULL,
	[IMP_CREATED_DATE] [datetime] NULL,
	[IMP_UPDATED_DATE] [datetime] NULL,
	[IMP_CREATED_BY] [nvarchar](255) NULL,
	[IMP_DELETED_DATE] [datetime] NULL
) ON [PRIMARY]
END
GO