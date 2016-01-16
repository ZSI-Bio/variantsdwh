CREATE TABLE dwh.dim_genomic_position_ensembl 
  ( 
     gp_id                      BIGINT, 
     gp_chr                     STRING, 
     gp_start                   INT, 
     gp_stop                    INT, 
     gp_exon_num                INT, 
     gp_exon_count              INT, 
     gp_trans_id                STRING, 
     iscanonical                BOOLEAN, 
     ismerged                   BOOLEAN, 
     gp_gene_id                 STRING, 
     gp_hgnc_approved_gene_name STRING, 
     gp_cyto                    STRING 
  ); 

CREATE TABLE dwh.dim_geography 
  ( 
     geo_id                 INT, 
     geo_code               STRING, 
     geo_country_name_en    STRING, 
     geo_region_id          INT, 
     geo_region_name_en     STRING, 
     geo_sub_region_id      INT, 
     geo_sub_region_name_en STRING 
  ); 

CREATE TABLE dwh.dim_disease 
  ( 
     d_id      BIGINT, 
     d_omim_id STRING, 
     d_disease STRING 
  ); 

CREATE TABLE dwh.dim_variant_predictions 
  ( 
     v_id                        BIGINT, 
     v_chr                       STRING, 
     v_pos                       INT, 
     v_alt                       STRING, 
     v_synonymic                 STRING, 
     v_rs_dbsnp142               STRING, 
     v_transcript_id             STRING, 
     v_sift_score                FLOAT, 
     v_sift_rankscore            FLOAT, 
     v_sift_pred                 STRING, 
     v_fathmm_score              FLOAT, 
     v_fathmm_rankscore          FLOAT, 
     v_fathmm_pred               STRING, 
     v_metasvm_score             FLOAT, 
     v_metasvm_rankscore         FLOAT, 
     v_metasvm_pred              STRING, 
     v_cadd_score                FLOAT, 
     v_cadd_rankscore            FLOAT, 
     v_cadd_phred                FLOAT, 
     v_metalr_score              FLOAT, 
     v_metalr_rankscore          FLOAT, 
     v_metalr_pred               STRING, 
     v_polyphen2_hvar_score      FLOAT, 
     v_polyphen2_hvar_rankscore  FLOAT, 
     v_polyphen2_hvar_pred       STRING, 
     v_mutation_taster_score     FLOAT, 
     v_mutation_taster_rankscore FLOAT, 
     v_mutation_taster_pred      STRING,
     v_chr_2 string,
     v_hg19_pos int,
     v_reference string,
     v_ucsc_name string
  );
  
  
  
CREATE TABLE dwh.fact(
   f_sample_id bigint,
   f_geo_id bigint,
   f_d_id bigint,
   f_ensembl_gp_id bigint,
   f_refseq_gp_id bigint,
   f_v_id bigint,
   f_variant_name string,
   f_chr string,
   f_pos bigint,
   f_ref string,
   f_alt string,
   f_alter_depth int,
   f_total_depth int,
   f_genotype string);
  
  
  
CREATE TABLE dwh.fact_agg_genotypes_counts 
  ( 
     f_ensembl_gp_id         BIGINT, 
     f_refseq_gp_id          BIGINT, 
     f_v_id                  BIGINT, 
     f_variant_name          STRING, 
     f_pos                   BIGINT, 
     f_ref                   STRING, 
     f_alt                   STRING, 
     f_d_id                  BIGINT, 
     f_sum_total_depth       BIGINT, 
     f_sum_alter_depth       BIGINT, 
     f_cnt_alter             BIGINT,  
     geo_region_id           BIGINT, 
     f_country_total_samples BIGINT, 
     f_region_total_samples  BIGINT, 
     f_total_samples         BIGINT, 
     f_genotype              STRING, 
     f_chr                   STRING, 
     f_geo_id                BIGINT 
  ) ;  
  

CREATE TABLE dwh.fact_agg_genotypes_counts_dims 
  ( 
     f_ensembl_gp_id             BIGINT, 
     f_refseq_gp_id              BIGINT, 
     f_v_id                      BIGINT, 
     f_variant_name              STRING, 
     f_pos                       BIGINT, 
     f_ref                       STRING, 
     f_alt                       STRING, 
     f_d_id                      BIGINT, 
     f_sum_total_depth           BIGINT, 
     f_sum_alter_depth           BIGINT, 
     f_cnt_alter                 BIGINT, 
     gp_id                       BIGINT, 
     gp_chr                      STRING, 
     gp_start                    INT, 
     gp_stop                     INT, 
     gp_exon_num                 INT, 
     gp_exon_count               INT, 
     gp_trans_id                 STRING, 
     iscanonical                 BOOLEAN, 
     ismerged                    BOOLEAN, 
     gp_gene_id                  STRING, 
     gp_hgnc_approved_gene_name  STRING, 
     gp_cyto                     STRING, 
     geo_id                      INT, 
     geo_code                    STRING, 
     geo_country_name_en         STRING, 
     geo_region_id               INT, 
     geo_region_name_en          STRING, 
     geo_sub_region_id           INT, 
     geo_sub_region_name_en      STRING, 
     f_country_total_samples     BIGINT, 
     f_region_total_samples      BIGINT, 
     f_total_samples             BIGINT, 
     v_id                        BIGINT, 
     v_chr                       STRING, 
     v_pos                       INT, 
     v_alt                       STRING, 
     v_synonymic                 STRING, 
     v_rs_dbsnp142               STRING, 
     v_transcript_id             STRING, 
     v_sift_score                FLOAT, 
     v_sift_rankscore            FLOAT, 
     v_sift_pred                 STRING, 
     v_fathmm_score              FLOAT, 
     v_fathmm_rankscore          FLOAT, 
     v_fathmm_pred               STRING, 
     v_metasvm_score             FLOAT, 
     v_metasvm_rankscore         FLOAT, 
     v_metasvm_pred              STRING, 
     v_cadd_score                FLOAT, 
     v_cadd_rankscore            FLOAT, 
     v_cadd_phred                FLOAT, 
     v_metalr_score              FLOAT, 
     v_metalr_rankscore          FLOAT, 
     v_metalr_pred               STRING, 
     v_polyphen2_hvar_score      FLOAT, 
     v_polyphen2_hvar_rankscore  FLOAT, 
     v_polyphen2_hvar_pred       STRING, 
     v_mutation_taster_score     FLOAT, 
     v_mutation_taster_rankscore FLOAT, 
     v_mutation_taster_pred      STRING, 
     d_id                        BIGINT, 
     d_omim_id                   STRING, 
     d_disease                   STRING, 
     f_genotype                  STRING, 
     f_chr                       STRING, 
     f_geo_id                    BIGINT 
  );   