{{
    config(
        materialized='incremental'
    )
}}

WITH pp_meta_data AS (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_fagsak AS (
    select * from {{ ref ('fam_pp_fagsak') }}
),

-- Hent diagnoser fra meldinger som ikke gjelder OLP
diagnoser AS (
select * from pp_meta_data,
  json_table(melding, '$'
    columns (
        vedtaks_tidspunkt   TIMESTAMP PATH '$.vedtakstidspunkt'
        ,ytelse_type VARCHAR2(255 CHAR) PATH '$.ytelseType'
        ,nested              PATH '$.diagnosekoder[*]' 
        columns (
            kode              VARCHAR2(255 CHAR) PATH '$.kode'
            ,type             VARCHAR2(255 CHAR) PATH '$.type'
      )
    )
  )  j
    WHERE ytelse_type != 'OLP'
      AND kode IS NOT NULL
),

-- Hent OLP-diagnoser med tilhørende perioder
olp_diagnoser AS (
	select * from pp_meta_data,
	  json_table(melding, '$'
        COLUMNS (
            vedtaks_tidspunkt TIMESTAMP PATH '$.vedtakstidspunkt',
            ytelse_type VARCHAR2(255 CHAR) PATH '$.ytelseType',
            NESTED PATH '$.diagnosekoder[*]'
            COLUMNS (
                kode VARCHAR2(255 CHAR) PATH '$.kode',
                type VARCHAR2(255 CHAR) PATH '$.type',
                NESTED PATH '$.perioder[*]'
                COLUMNS (
                    fom DATE PATH '$.fom',
                    tom DATE PATH '$.tom'
                )
            )
        )
    ) j
    WHERE ytelse_type = 'OLP'
      AND kode IS NOT NULL
),

-- Koble diagnoser til fagsak
diagnoser_med_fagsak AS (
    SELECT
        d.kode,
        d.type,
        f.pk_pp_fagsak AS fk_pp_fagsak,
        f.behandlings_id,
        f.saksnummer,
        f.vedtaks_tidspunkt
    FROM diagnoser d
    JOIN pp_fagsak f
        ON f.kafka_offset = d.kafka_offset
),

-- Finn diagnoser fra tidligere behandlinger i samme sak
diagnosehistorikk AS (
    SELECT
        ny.saksnummer,
        ny.behandlings_id AS siste_behandlings_id,
        tidligere.behandlings_id,
        tidligere_diagnose.type,
        tidligere_diagnose.kode
    FROM diagnoser_med_fagsak ny
    JOIN pp_fagsak tidligere
        ON tidligere.saksnummer = ny.saksnummer
        AND tidligere.vedtaks_tidspunkt < ny.vedtaks_tidspunkt
    JOIN fam_pp_diagnose tidligere_diagnose
        ON tidligere_diagnose.fk_pp_fagsak = tidligere.pk_pp_fagsak
        AND tidligere_diagnose.type = ny.type
        AND tidligere_diagnose.kode = ny.kode

    UNION ALL

    SELECT
        saksnummer,
        behandlings_id,
        behandlings_id,
        type,
        kode
    FROM diagnoser_med_fagsak
),

-- Tell antall behandlinger per diagnose
diagnosefrekvens AS (
    SELECT
        saksnummer,
        siste_behandlings_id,
        type,
        kode,
        COUNT(DISTINCT behandlings_id) AS antall_behandlinger
    FROM diagnosehistorikk
    GROUP BY
        saksnummer,
        siste_behandlings_id,
        type,
        kode
),

-- Finn minste diagnosefrekvens per behandling
min_diagnosefrekvens AS (
    SELECT
        siste_behandlings_id,
        MIN(antall_behandlinger) AS min_antall_behandlinger
    FROM diagnosefrekvens
    GROUP BY siste_behandlings_id
),

-- flagg for siste diagnose
diagnoser_med_flagg AS (
    SELECT
        d.kode,
        d.type,
        d.fk_pp_fagsak,
        d.behandlings_id,
        d.saksnummer,
        d.vedtaks_tidspunkt,
        f.antall_behandlinger,
        mf.min_antall_behandlinger,
        CASE
            WHEN NVL(f.antall_behandlinger, 0) = 0 THEN 'J'
            WHEN f.antall_behandlinger <= mf.min_antall_behandlinger THEN 'J'
            ELSE 'N'
        END AS siste_diagnose_flagg
    FROM diagnoser_med_fagsak d
    LEFT JOIN diagnosefrekvens f
        ON f.saksnummer = d.saksnummer
        AND f.siste_behandlings_id = d.behandlings_id
        AND f.kode = d.kode
        AND f.type = d.type
    LEFT JOIN min_diagnosefrekvens mf
        ON mf.siste_behandlings_id = f.siste_behandlings_id
),

-- Koble diagnoser til dimensjonen
diagnoser_med_dimensjon AS (
    SELECT
        d.kode,
        d.type,
        d.fk_pp_fagsak,
        dim.pk_dim_diagnose AS fk_dim_diagnose,
        d.siste_diagnose_flagg
    FROM diagnoser_med_flagg d
    JOIN {{ source( 'dt_p','dim_diagnose') }}  dim
        ON dim.diagnose_kode = d.kode
       AND UPPER(dim.diagnose_tabell) = d.type
       AND d.vedtaks_tidspunkt BETWEEN dim.gyldig_fra_dato AND dim.gyldig_til_dato
),

-- Koble OLP-diagnoser til fagsak og dimensjon
olp_med_dimensjon AS (
    SELECT
        d.kode,
        d.type,
        f.pk_pp_fagsak AS fk_pp_fagsak,
        dim.pk_dim_diagnose AS fk_dim_diagnose,
        d.fom,
        d.tom
    FROM olp_diagnoser d
    JOIN pp_fagsak f
        ON f.kafka_offset = d.kafka_offset
    JOIN {{ source( 'dt_p','dim_diagnose') }}  dim
        ON dim.diagnose_kode = d.kode
       AND UPPER(dim.diagnose_tabell) = d.type
       AND d.vedtaks_tidspunkt BETWEEN dim.gyldig_fra_dato AND dim.gyldig_til_dato
),

-- Samle alle diagnoser i samme struktur
alle_diagnoser AS (
    SELECT
        kode,
        type,
        fk_pp_fagsak,
        fk_dim_diagnose,
        siste_diagnose_flagg,
        CAST(NULL AS DATE) AS fom,
        CAST(NULL AS DATE) AS tom
    FROM diagnoser_med_dimensjon

    UNION ALL

    SELECT
        kode,
        type,
        fk_pp_fagsak,
        fk_dim_diagnose,
        CAST(NULL AS VARCHAR2(1)) AS siste_diagnose_flagg,
        fom,
        tom
    FROM olp_med_dimensjon
)

SELECT
    dvh_fam_pp.hibernate_sequence.NEXTVAL AS pk_pp_diagnose,
    kode,
    type,
    fk_pp_fagsak,
    LOCALTIMESTAMP AS lastet_dato,
    fk_dim_diagnose,
    siste_diagnose_flagg,
    fom,
    tom
FROM alle_diagnoser