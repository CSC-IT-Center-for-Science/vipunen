IF NOT EXISTS (select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_koulutusluokitus') BEGIN

CREATE TABLE [dbo].[sa_koulutusluokitus](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[koodi] [nvarchar](6) NOT NULL,
	[nimi] [nvarchar](200) NOT NULL,
	[nimi_sv] [nvarchar](200) NULL,
	[nimi_en] [nvarchar](200) NULL,
	[alkupvm] [date] NULL,
	[loppupvm] [date] NULL,
	[koulutusaste2002koodi] [nvarchar](2) NULL,
	[koulutusaste2002nimi] [nvarchar](200) NULL,
	[koulutusaste2002nimi_sv] [nvarchar](200) NULL,
	[koulutusaste2002nimi_en] [nvarchar](200) NULL,
	[koulutusala2002koodi] [nvarchar](3) NULL,
	[koulutusala2002nimi] [nvarchar](200) NULL,
	[koulutusala2002nimi_sv] [nvarchar](200) NULL,
	[koulutusala2002nimi_en] [nvarchar](200) NULL,
	[opintoala2002koodi] [nvarchar](3) NULL,
	[opintoala2002nimi] [nvarchar](200) NULL,
	[opintoala2002nimi_sv] [nvarchar](200) NULL,
	[opintoala2002nimi_en] [nvarchar](200) NULL,
	[koulutusaste1995koodi] [nvarchar](2) NULL,
	[koulutusaste1995nimi] [nvarchar](200) NULL,
	[koulutusaste1995nimi_sv] [nvarchar](200) NULL,
	[koulutusaste1995nimi_en] [nvarchar](200) NULL,
	[koulutusala1995koodi] [nvarchar](2) NULL,
	[koulutusala1995nimi] [nvarchar](200) NULL,
	[koulutusala1995nimi_sv] [nvarchar](200) NULL,
	[koulutusala1995nimi_en] [nvarchar](200) NULL,
	[opintoala1995koodi] [nvarchar](2) NULL,
	[opintoala1995nimi] [nvarchar](200) NULL,
	[opintoala1995nimi_sv] [nvarchar](200) NULL,
	[opintoala1995nimi_en] [nvarchar](200) NULL,
	[tutkintokoodi] [nvarchar](3) NULL,
	[tutkintonimi] [nvarchar](200) NULL,
	[tutkintonimi_sv] [nvarchar](200) NULL,
	[tutkintonimi_en] [nvarchar](200) NULL,
	[tutkintotyyppikoodi] [nvarchar](2) NULL,
	[tutkintotyyppinimi] [nvarchar](200) NULL,
	[tutkintotyyppinimi_sv] [nvarchar](200) NULL,
	[tutkintotyyppinimi_en] [nvarchar](200) NULL,
	[koulutustyyppikoodi] [nvarchar](3) NULL,
	[koulutustyyppinimi] [nvarchar](200) NULL,
	[koulutustyyppinimi_sv] [nvarchar](200) NULL,
	[koulutustyyppinimi_en] [nvarchar](200) NULL,
	[isced2011koulutusastekoodi] [nvarchar](2) NULL,
	[isced2011koulutusastenimi] [nvarchar](200) NULL,
	[isced2011koulutusastenimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusastenimi_en] [nvarchar](200) NULL,
	[isced2011koulutusastetaso1koodi] [nvarchar](2) NULL,
	[isced2011koulutusastetaso1nimi] [nvarchar](200) NULL,
	[isced2011koulutusastetaso1nimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusastetaso1nimi_en] [nvarchar](200) NULL,
	[isced2011koulutusastetaso2koodi] [nvarchar](2) NULL,
	[isced2011koulutusastetaso2nimi] [nvarchar](200) NULL,
	[isced2011koulutusastetaso2nimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusastetaso2nimi_en] [nvarchar](200) NULL,
	[isced2011koulutusalataso1koodi] [nvarchar](2) NULL,
	[isced2011koulutusalataso1nimi] [nvarchar](200) NULL,
	[isced2011koulutusalataso1nimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusalataso1nimi_en] [nvarchar](200) NULL,
	[isced2011koulutusalataso2koodi] [nvarchar](3) NULL,
	[isced2011koulutusalataso2nimi] [nvarchar](200) NULL,
	[isced2011koulutusalataso2nimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusalataso2nimi_en] [nvarchar](200) NULL,
	[isced2011koulutusalataso3koodi] [nvarchar](4) NULL,
	[isced2011koulutusalataso3nimi] [nvarchar](200) NULL,
	[isced2011koulutusalataso3nimi_sv] [nvarchar](200) NULL,
	[isced2011koulutusalataso3nimi_en] [nvarchar](200) NULL,
	[okmohjauksenalakoodi] [nvarchar](2) NULL,
	[okmohjauksenalanimi] [nvarchar](200) NULL,
	[okmohjauksenalanimi_sv] [nvarchar](200) NULL,
	[okmohjauksenalanimi_en] [nvarchar](200) NULL,
	[loadtime] [datetime2](4) NOT NULL,
	[source] [nvarchar](255) NULL,
	[username] [nvarchar](128) NOT NULL,
 CONSTRAINT [PK__sa_koulutusluokitus] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)
)
;--GO

ALTER TABLE [dbo].[sa_koulutusluokitus] ADD  CONSTRAINT [DF__sa_koulutusluokitus__loadtime]  DEFAULT (getdate()) FOR [loadtime]
;--GO
ALTER TABLE [dbo].[sa_koulutusluokitus] ADD  CONSTRAINT [DF__sa_koulutusluokitus__username]  DEFAULT (suser_name()) FOR [username]
;--GO

END
