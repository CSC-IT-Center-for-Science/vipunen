IF NOT EXISTS (
  select *
  from INFORMATION_SCHEMA.ROUTINES
  where ROUTINE_TYPE='PROCEDURE'
  and ROUTINE_SCHEMA='dbo'
  and ROUTINE_NAME='p_lataa_f_kotimaiset_julkaisut_tilasto'
) BEGIN
-- wrap in exec as create procedure must be first call in batch... (the if and begin is too much)
exec('
CREATE PROCEDURE dbo.p_lataa_f_kotimaiset_julkaisut_tilasto AS

TRUNCATE TABLE dbo.f_kotimaiset_julkaisut_tilasto

INSERT INTO dbo.f_kotimaiset_julkaisut_tilasto

SELECT
  -1 AS d_sektori_id,
  coalesce(d2_1.id,d2_2.id,d2_3.id,-1) AS d_organisaatio_id,
  coalesce(d3.id,-1) AS d_julkaisufoorumitaso_id,
  coalesce(d4.id,-1) AS d_julkaisutyyppi_id,
  coalesce(d5.id,-1) AS d_julkaisun_kansainvalisyys_id,
  coalesce(d6.id,-1) AS d_kansainvalinen_yhteisjulkaisu_id,
  coalesce(d7.id,-1) AS d_tieteenala_id,
  f.julkaisuVuosi AS d_tilastovuosi,

  cast(1 as int) AS lukumaara,

  ''ETL: p_lataa_f_kotimaiset_julkaisut_tilasto'' AS source
FROM VIPUNEN_SA.dbo.sa_virta_jtp_tkjulkaisut f
--todo: sektori
LEFT JOIN dbo.d_organisaatio d2_1 ON d2_1.organisaatio_avain like ''oppilaitosnumero_%'' and d2_1.organisaatio_koodi = f.organisaatioTunnus --and len(f.organisaatioTunnus)=5
LEFT JOIN dbo.d_organisaatio d2_2 ON d2_2.organisaatio_avain like ''koulutustoimija_%'' and d2_2.organisaatio_koodi = f.organisaatioTunnus --and len(f.organisaatioTunnus)=9
LEFT JOIN dbo.d_organisaatio d2_3 ON d2_3.organisaatio_avain like ''tutkimusorganisaatio_%'' and d2_3.organisaatio_koodi = f.organisaatioTunnus
LEFT JOIN dbo.d_julkaisufoorumitaso d3 ON d3.julkaisufoorumitaso_koodi = coalesce(f.jufoLuokkaKoodi,''-1'') --nb! puuttuva arvo on -1=arvioitavana (pitäisi olla tyhjä, mutta koodistopalvelu ei salli)
LEFT JOIN dbo.d_julkaisutyyppi d4 ON d4.julkaisutyyppi_koodi = f.julkaisuntyyppiKoodi
LEFT JOIN dbo.d_julkaisun_kansainvalisyys d5 ON d5.julkaisun_kansainvalisyys_koodi = f.julkaisunKansainvalisyysKytkin
LEFT JOIN dbo.d_kansainvalinen_yhteisjulkaisu d6 ON d6.kansainvalinen_yhteisjulkaisu_koodi = f.yhteisjulkaisuKVKytkin
LEFT JOIN dbo.d_tieteenala d7 ON d7.tieteenala_koodi = replace(substring(
    f.tieteenalaTKs
    ,charindex(''tieteenala'',f.tieteenalaTKs)+13
    ,4
  ),'','','''')
')
END
