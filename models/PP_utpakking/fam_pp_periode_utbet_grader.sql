{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_perioder as (
  select * from {{ref ('fam_pp_perioder')}}
),

pre_final as (
  select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested PATH '$.perioder[*]' columns (
      dato_fom         DATE PATH '$.fom'
      ,dato_tom         DATE PATH '$.tom'
      ,nested PATH '$.utbetalingsgrader[*]' columns (
         aktivitet_status       VARCHAR2(255 CHAR) PATH '$.aktivitetStatus'
        ,arbeidsforhold_aktorid VARCHAR2(255 CHAR) PATH '$.arbeidsforhold.aktørId'
        ,arbeidsforhold_id      VARCHAR2(255 CHAR) PATH '$.arbeidsforhold.arbeidsforholdId'
        ,arbeidsforhold_orgnr   VARCHAR2(255 CHAR) PATH '$.arbeidsforhold.organisasjonsnummer'
        ,arbeidsforhold_type    VARCHAR2(255 CHAR) PATH '$.arbeidsforhold.type'
        ,dagsats                NUMBER PATH '$.dagsats'
        ,faktisk_arbeidstid     VARCHAR2(255 CHAR) PATH '$.faktiskArbeidstid'
        ,normal_arbeidstid      VARCHAR2(255 CHAR) PATH '$.normalArbeidstid'
        ,utbetalingsgrad        NUMBER PATH '$.utbetalingsgrad'
        ,bruker_er_mottaker     VARCHAR2(255 CHAR) PATH '$.brukerErMottaker'
        )
      )
    )
  ) j
  where utbetalingsgrad is not null and bruker_er_mottaker is not null
),

final as (
  select
    p.aktivitet_status
    ,p.arbeidsforhold_aktorid
    ,p.arbeidsforhold_id
    ,p.arbeidsforhold_orgnr
    ,p.arbeidsforhold_type
    ,p.dagsats
    ,p.faktisk_arbeidstid
    ,p.normal_arbeidstid
    ,p.utbetalingsgrad
    ,CASE
      when p.bruker_er_mottaker = 'true' then 1
      when p.bruker_er_mottaker = 'false' then 0
    end bruker_er_mottaker
    ,perioder.pk_pp_perioder as FK_PP_PERIODER
  from pre_final p
  join pp_perioder perioder
  on p.kafka_offset = perioder.kafka_offset
  where p.dato_fom = perioder.dato_fom
  and p.dato_tom = perioder.dato_tom
)

select
  dvh_fam_pp.hibernate_sequence.nextval as PK_PP_PERIODE_UTBET_GRADER
  ,ARBEIDSFORHOLD_AKTORID
  ,ARBEIDSFORHOLD_ID
  ,ARBEIDSFORHOLD_ORGNR
  ,ARBEIDSFORHOLD_TYPE
  ,DAGSATS
  ,NORMAL_ARBEIDSTID
  ,FAKTISK_ARBEIDSTID
  ,UTBETALINGSGRAD
  ,FK_PP_PERIODER
  ,BRUKER_ER_MOTTAKER
  ,localtimestamp as LASTET_DATO
  ,aktivitet_status
from final


