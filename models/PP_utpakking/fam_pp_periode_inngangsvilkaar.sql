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
        nested            PATH '$.perioder[*]' columns (
        dato_fom          DATE PATH '$.fom'
        ,dato_tom         DATE PATH '$.tom'
        ,nested           PATH '$.inngangsvilkår[*]' columns (
          utfall          VARCHAR2(255 CHAR)  PATH '$.utfall'
          ,vilkaar        VARCHAR2(255 CHAR)  PATH '$.vilkår'
          )
        )
      )
    ) j
  where utfall is not null
),

final as (
  select
    p.utfall
    ,p.vilkaar
    ,p.dato_fom
    ,p.dato_tom
    ,p.kafka_offset
    ,periode.pk_pp_perioder as FK_PP_PERIODER
  from pre_final p
  join pp_perioder periode
  on p.kafka_offset = periode.kafka_offset
  and p.DATO_FOM = periode.DATO_FOM
  AND p.DATO_TOM = periode.DATO_TOM
)

select
  dvh_fam_pp.hibernate_sequence.nextval as PK_PP_PERIODE_INNGANGSVILKAAR
  ,UTFALL
  ,VILKAAR
  ,FK_PP_PERIODER
  ,localtimestamp as LASTET_DATO
  ,dato_fom
  ,dato_tom
  ,kafka_offset
from final