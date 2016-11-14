IF EXISTS (
  select * from INFORMATION_SCHEMA.COLUMNS
  where TABLE_SCHEMA='dbo' and TABLE_NAME='sa_virta_otp_yoavoimenopintopisteetjarj'
  and COLUMN_NAME='oppilaitostyyppi'
  and DATA_TYPE='varchar' and CHARACTER_MAXIMUM_LENGTH='2'
) BEGIN
ALTER TABLE dbo.sa_virta_otp_yoavoimenopintopisteetjarj ALTER COLUMN oppilaitostyyppi varchar(100)
END
