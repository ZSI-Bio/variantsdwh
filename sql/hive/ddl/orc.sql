CREATE TABLE `fact_new_orc`(
 `f_sample_id` bigint,
 `f_geo_id` bigint,
 `f_d_id` bigint,
 `f_ensembl_gp_id` bigint,
 `f_refseq_gp_id` bigint,
 `f_v_id` bigint,
 `f_variant_name` string,
 `f_chr` string,
 `f_pos` bigint,
 `f_ref` string,
 `f_alt` string,
 `f_alter_depth` int,
 `f_total_depth` int,
 `f_genotype` string
STORED AS ORC;

CREATE TABLE `dim_geography_orc`(
   `geo_id` int,
   `geo_code` string,
   `geo_country_name_en` string,
   `geo_region_id` int,
   `geo_region_name_en` string,
   `geo_sub_region_id` int,
   `geo_sub_region_name_en` string)
STORED AS ORC;

CREATE TABLE `dim_genomic_position_ensembl_orc`(
   `gp_id` bigint,
   `gp_chr` string,
   `gp_start` int,
   `gp_stop` int,
   `gp_exon_num` int,
   `gp_exon_count` int,
   `gp_trans_id` string,
   `iscanonical` boolean,
   `ismerged` boolean,
   `gp_gene_id` string,
   `gp_hgnc_approved_gene_name` string,
   `gp_cyto` string)
STORED AS ORC;

CREATE TABLE `dim_genomic_position_refseq_orc`(
   `gp_id` bigint,
   `gp_chr` string,
   `gp_start` int,
   `gp_stop` int,
   `gp_exon_num` int,
   `gp_exon_count` int,
   `gp_trans_id` string,
   `iscanonical` boolean,
   `ismerged` boolean,
   `gp_gene_id` string,
   `gp_hgnc_approved_gene_name` string,
   `gp_cyto` string)
STORED AS ORC;

CREATE TABLE `dim_disease`(
   `d_id` bigint,
   `d_omim_id` string,
   `d_disease` string)
 STORED AS ORC;

CREATE TABLE `dim_variant_predictions_orc`(
   `v_id` bigint,
   `v_chr` string,
   `v_pos` int,
   `v_alt` string,
   `v_synonymic` string,
   `v_rs_dbsnp142` string,
   `v_transcript_id` string,
   `v_sift_score` float,
   `v_sift_rankscore` float,
   `v_sift_pred` string,
   `v_fathmm_score` float,
   `v_fathmm_rankscore` float,
   `v_fathmm_pred` string,
   `v_metasvm_score` float,
   `v_metasvm_rankscore` float,
   `v_metasvm_pred` string,
   `v_cadd_score` float,
   `v_cadd_rankscore` float,
   `v_cadd_phred` float,
   `v_metalr_score` float,
   `v_metalr_rankscore` float,
   `v_metalr_pred` string,
   `v_polyphen2_hvar_score` float,
   `v_polyphen2_hvar_rankscore` float,
   `v_polyphen2_hvar_pred` string,
   `v_mutation_taster_score` float,
   `v_mutation_taster_rankscore` float,
   `v_mutation_taster_pred` string,
   `v_variant_name` string)
STORED AS ORC


CREATE TABLE fact_agg_genotypes_counts_orc STORED AS ORC AS
SELECT
g.f_ensembl_gp_id    ,
g.f_refseq_gp_id     ,
g.f_v_id             ,
g.f_variant_name     ,
g.f_pos              ,
g.f_ref              ,
g.f_alt              ,
g.f_d_id             ,
g.f_sum_total_depth  ,
g.f_sum_alter_depth  ,
g.f_cnt_alter        ,
g.geo_country_name_en    ,
g.geo_region_id          ,
g.geo_region_name_en     ,
t.f_country_total_samples        ,
t2.f_region_total_samples,
t3.f_total_samples,
g.f_genotype         ,
g.f_chr              ,
g.f_geo_id
FROM   (SELECT f_geo_id,
               geo_country_name_en    ,
               geo_region_id          ,
               geo_region_name_en     ,
               f_ensembl_gp_id,
               f_refseq_gp_id,
               f_v_id,
               f_variant_name,
               f_chr,
               f_pos,
               f_ref,
               f_alt,
               f_genotype,
               f_d_id,
               Sum(f_total_depth)          f_sum_total_depth,
               Sum(f_alter_depth)          f_sum_alter_depth,
               Count(DISTINCT f_sample_id) AS f_cnt_alter
        FROM   dwh.fact_new
        JOIN dwh.dim_geography ON f_geo_id = geo_id
        WHERE  f_ensembl_gp_id IS NOT NULL
        GROUP  BY f_geo_id,
                  geo_country_name_en    ,
                  geo_region_id          ,
                  geo_region_name_en     ,
                  f_ensembl_gp_id,
                  f_refseq_gp_id,
                  f_v_id,
                  f_variant_name,
                  f_chr,
                  f_pos,
                  f_ref,
                  f_alt,
                  f_genotype,
                  f_d_id) g
       JOIN (SELECT Count(DISTINCT f_sample_id) f_country_total_samples,
                    f_geo_id
             FROM   dwh.fact_new
             GROUP  BY f_geo_id) t
         ON ( t.f_geo_id = g.f_geo_id )
       JOIN (SELECT Count(DISTINCT f_sample_id) f_region_total_samples,
                    geo_region_id
             FROM   dwh.fact_new JOIN dwh.dim_geography ON f_geo_id = geo_id
             GROUP BY geo_region_id) t2
         ON ( t2.geo_region_id = g.geo_region_id )
         JOIN (SELECT Count(DISTINCT f_sample_id) f_total_samples
               FROM   dwh.fact_new) t3
           ON ( 1 = 1 );

  CREATE TABLE fact_agg_genotypes_counts_dims_orc STORED AS ORC AS
  SELECT
  f_ensembl_gp_id            ,
  f_refseq_gp_id             ,
  f_v_id                     ,
  f_variant_name             ,
  f_pos                      ,
  f_ref                      ,
  f_alt                      ,
  f_d_id                     ,
  f_sum_total_depth          ,
  f_sum_alter_depth          ,
  f_cnt_alter                ,
  gp_id                      ,
  gp_chr                     ,
  gp_start                   ,
  gp_stop                    ,
  gp_exon_num                ,
  gp_exon_count              ,
  gp_trans_id                ,
  iscanonical                ,
  ismerged                   ,
  gp_gene_id                 ,
  gp_hgnc_approved_gene_name ,
  gp_cyto                    ,
  geo_id                     ,
  dg.geo_code                   ,
  dg.geo_country_name_en        ,
  dg.geo_region_id              ,
  dg.geo_region_name_en         ,
  dg.geo_sub_region_id          ,
  dg.geo_sub_region_name_en     ,
  t.f_country_total_samples        ,
  t2.f_region_total_samples,
  t3.f_total_samples,
  v_id                       ,
  v_chr                      ,
  v_pos                      ,
  v_alt                      ,
  v_synonymic                ,
  v_rs_dbsnp142              ,
  v_transcript_id            ,
  v_sift_score               ,
  v_sift_rankscore           ,
  v_sift_pred                ,
  v_fathmm_score             ,
  v_fathmm_rankscore         ,
  v_fathmm_pred              ,
  v_metasvm_score            ,
  v_metasvm_rankscore        ,
  v_metasvm_pred             ,
  v_cadd_score               ,
  v_cadd_rankscore           ,
  v_cadd_phred               ,
  v_metalr_score             ,
  v_metalr_rankscore         ,
  v_metalr_pred              ,
  v_polyphen2_hvar_score     ,
  v_polyphen2_hvar_rankscore ,
  v_polyphen2_hvar_pred      ,
  v_mutation_taster_score    ,
  v_mutation_taster_rankscore,
  v_mutation_taster_pred     ,
  d_id                       ,
  d_omim_id                  ,
  d_disease                  ,
  f_genotype,
  g.f_chr              ,
  g.f_geo_id
  FROM   (SELECT f_geo_id,
                f_ensembl_gp_id,
                f_refseq_gp_id,
                f_v_id,
                f_variant_name,
                f_chr,
                f_pos,
                f_ref,
                f_alt,
                f_genotype,
                f_d_id,
                Sum(f_total_depth)          f_sum_total_depth,
                Sum(f_alter_depth)          f_sum_alter_depth,
                Count(DISTINCT f_sample_id) AS f_cnt_alter
         FROM   dwh.fact_new
         WHERE  f_ensembl_gp_id IS NOT NULL
         GROUP  BY f_geo_id,
                   f_ensembl_gp_id,
                   f_refseq_gp_id,
                   f_v_id,
                   f_variant_name,
                   f_chr,
                   f_pos,
                   f_ref,
                   f_alt,
                   f_genotype,
                   f_d_id) g
        LEFT OUTER JOIN dwh.dim_genomic_position_ensembl dge
                     ON ( g.f_ensembl_gp_id = dge.gp_id )
        --join dwh.dim_genomic_position_refseq dgr on (g.f_refseq_gp_id=dge.gp_id)
        LEFT OUTER JOIN dwh.dim_geography dg
                     ON ( g.f_geo_id = dg.geo_id )
        LEFT OUTER JOIN dwh.dim_variant_predictions vp
                     ON ( g.f_v_id = vp.v_id )
        LEFT OUTER JOIN dwh.dim_disease dd
                     ON ( g.f_d_id = dd.d_id )
        JOIN (SELECT Count(DISTINCT f_sample_id) f_country_total_samples,
                     f_geo_id
              FROM   dwh.fact_new
              GROUP  BY f_geo_id) t
          ON ( t.f_geo_id = g.f_geo_id )
        JOIN (SELECT Count(DISTINCT f_sample_id) f_region_total_samples,
                     geo_region_id
              FROM   dwh.fact_new JOIN dwh.dim_geography ON f_geo_id = geo_id
              GROUP BY geo_region_id) t2
          ON ( t2.geo_region_id = dg.geo_region_id )
        JOIN (SELECT Count(DISTINCT f_sample_id) f_total_samples
              FROM   dwh.fact_new) t3
          ON ( 1 = 1 )
