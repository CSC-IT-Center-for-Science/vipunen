-- nb! login tulee olla jo!
-- tässä lisätään siis user loginille, jolla tätä skriptiä luultavasti ajetaan, joten ei ihan mahdollinen tilanne

-- ajo onnistuu luonnollisesti vain tunnuksella jolla on oikeus luoda käyttäjiä
-- kierretään asia nyt sillä, että koko roska on tämän olemassaolo-iffin sisällä

IF NOT EXISTS (select * from sys.user_token where name='LJquicktest') BEGIN

CREATE USER [LJquicktest] FOR LOGIN [LJquicktest] WITH DEFAULT_SCHEMA=[dbo]
;--GO
ALTER ROLE [db_ddladmin] ADD MEMBER [LJquicktest]
;--GO
ALTER ROLE [db_datareader] ADD MEMBER [LJquicktest]
;--GO
ALTER ROLE [db_datawriter] ADD MEMBER [LJquicktest]
;--GO

END
