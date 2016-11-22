IF NOT EXISTS (select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='dbo' and TABLE_NAME='migration') BEGIN
CREATE TABLE dbo.migration(
	id int IDENTITY(1,1) NOT NULL,
	phase varchar(20) NOT NULL,
	number int NOT NULL,
	loadtime datetime NOT NULL,
	username varchar(30) NOT NULL,

)
;
ALTER TABLE dbo.migration ADD  CONSTRAINT DF_migration_loadtime  DEFAULT (getdate()) FOR loadtime
;
ALTER TABLE dbo.migration ADD  CONSTRAINT DF_migration_username  DEFAULT (suser_name()) FOR username
;
END
