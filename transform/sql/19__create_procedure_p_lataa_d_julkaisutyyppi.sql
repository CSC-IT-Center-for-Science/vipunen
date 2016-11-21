IF NOT EXISTS (
  select *
  from INFORMATION_SCHEMA.ROUTINES
  where ROUTINE_TYPE='PROCEDURE'
  and ROUTINE_SCHEMA='dbo'
  and ROUTINE_NAME='p_lataa_d_julkaisutyyppi'
) BEGIN
-- wrap in exec as create procedure must be first call in batch...
exec('
CREATE PROCEDURE dbo.p_lataa_d_julkaisutyyppi AS
MERGE dbo.d_julkaisutyyppi AS target
USING (
  SELECT koodi
  ,COALESCE(nimi, nimi_sv, nimi_en) AS nimi
  ,COALESCE(nimi_sv, nimi, nimi_en) AS nimi_sv
  ,COALESCE(nimi_en, nimi, nimi_sv) AS nimi_en
  ,julkaisunpaaluokkakoodi
  ,COALESCE(julkaisunpaaluokkanimi, julkaisunpaaluokkanimi_sv, julkaisunpaaluokkanimi_en) AS julkaisunpaaluokkanimi
  ,COALESCE(julkaisunpaaluokkanimi_sv, julkaisunpaaluokkanimi, julkaisunpaaluokkanimi_en) AS julkaisunpaaluokkanimi_sv
  ,COALESCE(julkaisunpaaluokkanimi_en, julkaisunpaaluokkanimi, julkaisunpaaluokkanimi_sv) AS julkaisunpaaluokkanimi_en
	  ,''ETL: p_lataa_d_julkaisutyyppi'' AS source
  FROM VIPUNEN_SA.dbo.sa_julkaisutyyppiluokitus
  ) AS src
ON target.julkaisutyyppi_koodi = src.koodi
WHEN MATCHED THEN
	UPDATE SET julkaisutyyppi_nimi_fi = src.nimi,
				julkaisutyyppi_nimi_sv = src.nimi_sv,
				julkaisutyyppi_nimi_en = src.nimi_en,
				julkaisunpaaluokka_koodi = src.julkaisunpaaluokkakoodi,
				julkaisunpaaluokka_nimi_fi = src.julkaisunpaaluokkanimi,
				julkaisunpaaluokka_nimi_sv = src.julkaisunpaaluokkanimi_sv,
				julkaisunpaaluokka_nimi_en = src.julkaisunpaaluokkanimi_en,
				target.source = src.source
WHEN NOT MATCHED THEN
	INSERT (
	  julkaisutyyppi_koodi,
      julkaisutyyppi_nimi_fi,
      julkaisutyyppi_nimi_sv,
      julkaisutyyppi_nimi_en,
	  julkaisunpaaluokka_koodi,
	  julkaisunpaaluokka_nimi_fi,
	  julkaisunpaaluokka_nimi_sv,
	  julkaisunpaaluokka_nimi_en,
      source
	)
	VALUES (
	  src.koodi, src.nimi, src.nimi_sv, src.nimi_en,
	  src.julkaisunpaaluokkakoodi, src.julkaisunpaaluokkanimi, src.julkaisunpaaluokkanimi_sv, src.julkaisunpaaluokkanimi_en,
	  src.source
	);
')
END
