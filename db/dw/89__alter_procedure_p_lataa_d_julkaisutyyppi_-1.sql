ALTER PROCEDURE dbo.p_lataa_d_julkaisutyyppi AS
if not exists (select * from dbo.d_julkaisutyyppi where id=-1) begin
  set identity_insert dbo.d_julkaisutyyppi on;
  insert into dbo.d_julkaisutyyppi (
    id,
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
  select
    -1,
    koodi,
    nimi,
    nimi_sv,
    nimi_en,
    koodi,
    nimi,
    nimi_sv,
    nimi_en,
    'ETL: p_lataa_d_julkaisutyyppi'
  from VIPUNEN_SA.dbo.sa_koodistot
  where koodisto='vipunenmeta'
  and koodi='-1'
  ;
  set identity_insert dbo.d_julkaisutyyppi off;
end else begin
  update d
  set julkaisutyyppi_koodi=s.koodi,
  julkaisutyyppi_nimi_fi=s.nimi,
  julkaisutyyppi_nimi_sv=s.nimi_sv,
  julkaisutyyppi_nimi_en=s.nimi_en,
  julkaisunpaaluokka_koodi=s.koodi,
  julkaisunpaaluokka_nimi_fi=s.nimi,
  julkaisunpaaluokka_nimi_sv=s.nimi_sv,
  julkaisunpaaluokka_nimi_en=s.nimi_en,
  source='ETL: p_lataa_d_julkaisutyyppi'
  from dbo.d_julkaisutyyppi d
  join VIPUNEN_SA.dbo.sa_koodistot s on s.koodi=d.julkaisutyyppi_koodi
  where s.koodisto='vipunenmeta'
  and s.koodi='-1'
  ;
end
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
	  ,'ETL: p_lataa_d_julkaisutyyppi' AS source
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
