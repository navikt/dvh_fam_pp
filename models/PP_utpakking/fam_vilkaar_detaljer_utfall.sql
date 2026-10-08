{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_periode_inngangsvilkaar as (
  select * from {{ref ('fam_pp_periode_inngangsvilkaar')}}
),

pre_final as (
  select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested                          PATH '$.perioder[*]' columns (
      dato_fom                        DATE  PATH '$.fom'
      ,dato_tom                       DATE  PATH '$.tom'
      ,nested                         PATH '$.inngangsvilkår[*]' columns (
      utfall                          VARCHAR2(255 CHAR) PATH '$.utfall'
      ,vilkaar                        VARCHAR2(255 CHAR) PATH '$.vilkår',
      nested                          PATH '$.detaljertUtfall[*]' columns (
      GJELDER_KRAVSTILLER             VARCHAR2(255 CHAR) PATH '$.gjelderKravstiller',
      GJELDER_AKTIVITET_TYPE          VARCHAR2(255 CHAR) PATH '$.gjelderAktivitetType',
      GJELDER_ORGANISASJONSNUMMER     VARCHAR2(255 CHAR) PATH '$.gjelderOrganisasjonsnummer',
      GJELDER_AKTOR_ID                VARCHAR2(255 CHAR) PATH '$.gjelderAktørId',
      GJELDER_ARBEIDSFORHOLD_ID       VARCHAR2(255 CHAR) PATH '$.gjelderArbeidsforholdId',
      det_utfall                      VARCHAR2(255 CHAR) PATH '$.utfall'
          )
        )
      )
    )
  ) j
  where GJELDER_ORGANISASJONSNUMMER is not null and GJELDER_AKTIVITET_TYPE is not null
),

final as (
  select 
    p.*
    ,pk_pp_periode_inngangsvilkaar as FK_PP_PERIODE_INNGANGSVILKAAR
  from pre_final p
  join pp_periode_inngangsvilkaar inn
  on p.kafka_offset = inn.kafka_offset
  and p.DATO_FOM = inn.DATO_FOM
  AND p.DATO_TOM = inn.DATO_TOM
  and p.utfall = inn.utfall
  and p.vilkaar = inn.vilkaar
)

select
  dvh_fam_pp.hibernate_sequence.nextval as PK_VILKAAR_DETALJERT_UTFALL
  ,GJELDER_KRAVSTILLER
  ,GJELDER_AKTIVITET_TYPE
  ,GJELDER_ORGANISASJONSNUMMER
  ,GJELDER_AKTOR_ID
  ,GJELDER_ARBEIDSFORHOLD_ID
  ,DET_UTFALL
  ,localtimestamp as LASTET_DATO
  ,FK_PP_PERIODE_INNGANGSVILKAAR
from final