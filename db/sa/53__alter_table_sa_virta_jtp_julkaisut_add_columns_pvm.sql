IF NOT EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_virta_jtp_julkaisut'
  and COLUMN_NAME='muutospvm'
) BEGIN
ALTER TABLE dbo.sa_virta_jtp_julkaisut ADD COLUMN muutospvm	varchar(50) null
END

IF NOT EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_virta_jtp_julkaisut'
  and COLUMN_NAME='luontipvm'
) BEGIN
ALTER TABLE dbo.sa_virta_jtp_julkaisut ADD COLUMN luontipvm	varchar(50) null
END
