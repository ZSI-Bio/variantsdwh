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
     v_mutation_taster_pred      STRING 
  );
  
  
  