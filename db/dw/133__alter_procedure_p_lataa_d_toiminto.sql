ALTER PROCEDURE dbo.p_lataa_d_toiminto AS

TRUNCATE TABLE dbo.d_toiminto

SET IDENTITY_INSERT dbo.d_toiminto ON
INSERT dbo.d_toiminto (id, toiminto_koodi, toiminto_nimi_fi, toiminto_nimi_sv, toiminto_nimi_en, loadtime, source, username)
VALUES 
(-1, N'-1', N'Tuntematon', N'Tuntematon', N'Tuntematon', CAST(N'2016-11-09T00:00:00.000' AS DateTime), N'manual', N'DWI\jrouhiai'),
(1, N'Koulutustoiminta', N'Koulutustoiminta', N'Koulutustoiminta', N'Koulutustoiminta', CAST(N'2016-11-10T08:00:33.513' AS DateTime), N'p_lataa_d_toiminto', N'DWI\jrouhiai'),
(2, N'Muu yhteiskunnallinen toiminta', N'Muu yhteiskunnallinen toiminta', N'Muu yhteiskunnallinen toiminta', N'Muu yhteiskunnallinen toiminta', CAST(N'2016-11-10T08:00:33.513' AS DateTime), N'p_lataa_d_toiminto', N'DWI\jrouhiai'),
(3, N'Toiminnot yhteens�', N'Toiminnot yhteens�', N'Toiminnot yhteens�', N'Toiminnot yhteens�', CAST(N'2016-11-10T08:00:33.513' AS DateTime), N'p_lataa_d_toiminto', N'DWI\jrouhiai'),
(4, N'Tutkimus- ja kehitystoiminta', N'Tutkimus- ja kehitystoiminta', N'Tutkimus- ja kehitystoiminta', N'Tutkimus- ja kehitystoiminta', CAST(N'2016-11-10T08:00:33.517' AS DateTime), N'p_lataa_d_toiminto', N'DWI\jrouhiai'),
(5, N'Yhteiskulut', N'Yhteiskulut', N'Yhteiskulut', N'Yhteiskulut', CAST(N'2016-11-10T08:00:33.517' AS DateTime), N'p_lataa_d_toiminto', N'DWI\jrouhiai')
SET IDENTITY_INSERT dbo.d_toiminto OFF