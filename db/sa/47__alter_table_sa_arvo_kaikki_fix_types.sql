IF NOT EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_arvo_kaikki'
  and COLUMN_NAME='tunnus'
  and DATA_TYPE='nchar' and CHARACTER_MAXIMUM_LENGTH='10'
) BEGIN
ALTER TABLE dbo.sa_arvo_kaikki ALTER COLUMN tunnus varchar(10)
END

IF NOT EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_arvo_kaikki'
  and COLUMN_NAME='kunta'
  and DATA_TYPE='nchar' and CHARACTER_MAXIMUM_LENGTH='10'
) BEGIN
ALTER TABLE dbo.sa_arvo_kaikki ALTER COLUMN kunta nvarchar(max)
END

IF NOT EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_arvo_kaikki'
  and COLUMN_NAME='koulutusmuoto'
  and DATA_TYPE='nchar' and CHARACTER_MAXIMUM_LENGTH='10'
) BEGIN
ALTER TABLE dbo.sa_arvo_kaikki ALTER COLUMN koulutusmuoto nvarchar(max)
END
