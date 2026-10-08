{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_fagsak AS (
  SELECT * FROM {{ ref ('fam_pp_fagsak') }}
),

pre_final as (
  select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested  PATH '$.perioder[*]' columns (
        beredskap                          VARCHAR2(255 CHAR) PATH '$.beredskap'
        ,brutto_beregningsgrunnlag         NUMBER PATH '$.bruttoBeregningsgrunnlag'
        ,dato_fom                          DATE  PATH '$.fom'
        ,dato_tom                          DATE  PATH '$.tom'
        ,gmt_andre_sokers_tilsyn           VARCHAR2(255 CHAR) PATH '$.graderingMotTilsyn.andreSøkeresTilsyn'
        ,gmt_etablert_tilsyn               NUMBER  PATH '$.graderingMotTilsyn.etablertTilsyn'
        ,gmt_overse_etablert_tilsyn_aarsak VARCHAR2(255 CHAR) PATH '$.graderingMotTilsyn.overseEtablertTilsynÅrsak'
        ,gmt_tilgjengelig_for_soker        NUMBER  PATH '$.graderingMotTilsyn.tilgjengeligForSøker'
        ,nattevaak                         VARCHAR2(255 CHAR) PATH '$.nattevåk'
        ,oppgitt_tilsyn                    VARCHAR2(255 CHAR) PATH '$.oppgittTilsyn'
        ,pleiebehov                        NUMBER  PATH '$.pleiebehov'
        ,sokers_tapte_timer                VARCHAR2(255 CHAR) PATH '$.søkersTapteTimer'
        ,utfall                            VARCHAR2(255 CHAR) PATH '$.utfall'
        ,uttaksgrad                        NUMBER  PATH '$.uttaksgrad'
        ,sokers_tapte_arbeidstid           NUMBER  PATH '$.søkersTapteArbeidstid'
        ,total_utbetalingsgrad             NUMBER  PATH '$.totalUtbetalingsgrad'
        ,total_utbetalingsgrad_fra_uttak   NUMBER  PATH '$.totalUtbetalingsgradFraUttak'
        ,total_utbetalingsgrad_etter_reduksjon_ved_tilkommet_inntekt  NUMBER  PATH '$.totalUtbetalingsgradEtterReduksjonVedTilkommetInntekt'
      )
    )
  ) j
  where dato_fom is not null
),

final as (
  select
    p.beredskap
    ,p.brutto_beregningsgrunnlag
    ,p.dato_fom
    ,p.dato_tom
    ,p.gmt_andre_sokers_tilsyn
    ,p.gmt_etablert_tilsyn
    ,p.gmt_overse_etablert_tilsyn_aarsak
    ,p.gmt_tilgjengelig_for_soker
    ,p.nattevaak
    ,p.oppgitt_tilsyn
    ,p.pleiebehov
    ,p.sokers_tapte_timer
    ,p.utfall
    ,p.kafka_offset
    ,p.uttaksgrad
    ,p.sokers_tapte_arbeidstid
    ,p.total_utbetalingsgrad
    ,p.total_utbetalingsgrad_fra_uttak
    ,p.total_utbetalingsgrad_etter_reduksjon_ved_tilkommet_inntekt
    ,f.pk_pp_fagsak as FK_PP_FAGSAK
  from pre_final p
  join pp_fagsak f
  on p.kafka_offset = f.kafka_offset
)

select
  dvh_fam_pp.hibernate_sequence.nextval as PK_PP_PERIODER
  ,BEREDSKAP
  ,BRUTTO_BEREGNINGSGRUNNLAG
  ,DATO_FOM
  ,DATO_TOM
  ,GMT_ANDRE_SOKERS_TILSYN
  ,GMT_ETABLERT_TILSYN
  ,GMT_OVERSE_ETABLERT_TILSYN_AARSAK
  ,GMT_TILGJENGELIG_FOR_SOKER
  ,NATTEVAAK
  ,OPPGITT_TILSYN
  ,PLEIEBEHOV
  ,SOKERS_TAPTE_TIMER
  ,SOKERS_TAPTE_ARBEIDSTID
  ,UTFALL
  ,UTTAKSGRAD
  ,FK_PP_FAGSAK
  ,localtimestamp as LASTET_DATO
  ,kafka_offset
  ,total_utbetalingsgrad
  ,total_utbetalingsgrad_fra_uttak
  ,total_utbetalingsgrad_etter_reduksjon_ved_tilkommet_inntekt
from final
