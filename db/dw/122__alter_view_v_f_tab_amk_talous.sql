ALTER VIEW dbo.v_f_tab_amk_talous AS
SELECT d1.amk_tunnus AS 'Ammattikorkeakoulun tunnus'
  ,d1.amk_nimi_fi AS 'Ammattikorkeakoulu'
  ,tilikausi AS 'Tilikausi'
  ,d2.ohjauksenala_nimi_fi AS 'Ohjauksenala'
  ,d1.amk_tunnus AS 'Yrkeshögskolas beteckning'
  ,d1.amk_nimi_sv AS 'Yrkeshögskola'
  ,tilikausi AS 'Finansår'
  ,d2.ohjauksenala_nimi_sv AS 'Utbildningsområde'
  ,d1.amk_tunnus AS 'Identifier of the university of applied science'
  ,d1.amk_nimi_en AS 'University of applied science'
  ,tilikausi AS 'Financial year'
  ,d2.ohjauksenala_nimi_en AS 'Field of education'
  ,d2.ohjauksenala_koodi AS 'Ohjauksenalakoodi'
  ,d2.ohjauksenala_koodi + ' ' + d2.ohjauksenala_nimi_fi AS 'Ohjauksenala koodilla'
  ,d3.tili_taso0_fi
  ,d3.tili_taso1_fi
  ,d3.tili_taso2_fi
  ,d3.tili_taso3_fi
  ,d3.tili_taso4_fi
  ,d3.tili_taso0_sv
  ,d3.tili_taso1_sv
  ,d3.tili_taso2_sv
  ,d3.tili_taso3_sv
  ,d3.tili_taso4_sv
  ,d3.tili_taso0_en
  ,d3.tili_taso1_en
  ,d3.tili_taso2_en
  ,d3.tili_taso3_en
  ,d3.tili_taso4_en
  ,d5.toiminto_nimi_fi
  ,d6.erittely_nimi_fi
  ,f.arvo
  FROM f_amk_talous f
INNER JOIN VIPUNEN_DW.dbo.d_amk d1 ON f.d_amk_id = d1.id
INNER JOIN VIPUNEN_DW.dbo.d_ohjauksenala d2 ON f.d_ohjauksenala_id = d2.id
INNER JOIN VIPUNEN_DW.dbo.d_tili d3 ON f.d_tili_id = d3.id
INNER JOIN VIPUNEN_DW.dbo.d_aineistotyyppi d4 ON f.d_aineistotyyppi_id = d4.id
INNER JOIN VIPUNEN_DW.dbo.d_toiminto d5 ON f.d_toiminto_id = d5.id
INNER JOIN VIPUNEN_DW.dbo.d_erittely d6 ON f.d_erittely_id = d6.id
