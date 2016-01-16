###Remarks:
1)Raw dataset for MonetDB was limited to 1*10^9 rows (approx. 1/4 of the dataset)

2)Hive does not support predicate pushdown on partitioned in case of Parquet format https://issues.apache.org/jira/browse/HIVE-11401

# Query 1

Allele frequencies - breakdown by ethnic groups

## SQL

###raw:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , geo_sample_cnt
     , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , SUM(genotype_val) as var_cnt
     , geo_sample_cnt
    --  , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_sample_id
                    , f_geo_id
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_new_pq_gzip) fact_new_orc_partitioned
JOIN dim_geography ON f_geo_id = geo_id 
JOIN (
      SELECT  geo_region_name_en as grn
            , COUNT(DISTINCT f_sample_id) as geo_sample_cnt
      FROM fact_new_pq_gzip JOIN dim_geography ON f_geo_id = geo_id
      GROUP BY geo_region_name_en) geo_sample_cnt 
ON geo_region_name_en = grn
GROUP BY f_chr, f_pos, f_alt, geo_region_name_en, geo_sample_cnt
LIMIT 10
) dummy
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , geo_sample_cnt
     , (cast (var_cnt as double) / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , SUM(genotype_val) as var_cnt
     , geo_sample_cnt
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_sample_id
                    , f_geo_id
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_new_orc_partitioned) fact_new_orc_partitioned
JOIN dim_geography ON f_geo_id = geo_id 
JOIN (
      SELECT  geo_region_name_en as grn
            , COUNT(DISTINCT f_sample_id) as geo_sample_cnt
      FROM fact_new_orc_partitioned JOIN dim_geography ON f_geo_id = geo_id
      GROUP BY geo_region_name_en) geo_sample_cnt 
ON geo_region_name_en = grn
GROUP BY f_chr, f_pos, f_alt, geo_region_name_en, geo_sample_cnt
LIMIT 10
) dummy
```


###aggr:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , f_region_total_samples
     , (var_cnt / (2 * f_region_total_samples)) as var_frequency
FROM
(
SELECT  f_chr
     , f_pos
     , f_alt
     , geo_region_id
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_region_total_samples
     FROM
     (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_d_id
                    , f_geo_id
                    , geo_region_id
                    , if(f_genotype='1/1',2,1) as genotype_val
                    , f_cnt_alter
                    , f_region_total_samples
      FROM fact_agg_genotypes_counts_pq_gzip) t
GROUP BY f_chr, f_pos, f_alt, geo_region_id, f_region_total_samples
) dummy
LEFT JOIN (SELECT DISTINCT geo_region_id, geo_region_name_en 
           FROM dim_geography) d 
ON d.geo_region_id = dummy.geo_region_id
LIMIT 10
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , f_region_total_samples
     , (cast (var_cnt as double) / (2 * f_region_total_samples)) as var_frequency
FROM
(
SELECT  f_chr
     , f_pos
     , f_alt
     , geo_region_id
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_region_total_samples
     FROM
     (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_d_id
                    , f_geo_id
                    , geo_region_id
                    , if(f_genotype='1/1',2,1) as genotype_val
                    , f_cnt_alter
                    , f_region_total_samples
      FROM fact_agg_genotypes_counts_pq_gzip) t
GROUP BY f_chr, f_pos, f_alt, geo_region_id, f_region_total_samples
) dummy
LEFT JOIN (SELECT DISTINCT geo_region_id, geo_region_name_en 
           FROM dim_geography) d 
ON d.geo_region_id = dummy.geo_region_id
LIMIT 10
```


aggr+denorm(nie ma różnicy w zapytaniu - aggr zdenormalizowana pod kątem dim_geography):

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , f_region_total_samples
     , (var_cnt / (2 * f_region_total_samples)) as var_frequency
FROM
(
SELECT  f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_region_total_samples
     FROM
     (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_d_id
                    , f_geo_id
                    , geo_region_name_en
                    , if(f_genotype='1/1',2,1) as genotype_val
                    , f_cnt_alter
                    , f_region_total_samples
      FROM fact_agg_genotypes_counts_dims_pq_gzip) t
GROUP BY f_chr, f_pos, f_alt, geo_region_name_en, f_region_total_samples
) dummy
LIMIT 10
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , var_cnt
     , f_region_total_samples
     , (cast (var_cnt as double) / (2 * f_region_total_samples)) as var_frequency
FROM
(
SELECT  f_chr
     , f_pos
     , f_alt
     , geo_region_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_region_total_samples
     FROM
     (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_d_id
                    , f_geo_id
                    , geo_region_name_en
                    , if(f_genotype='1/1',2,1) as genotype_val
                    , f_cnt_alter
                    , f_region_total_samples
      FROM fact_agg_genotypes_counts_dims_pq_gzip) t
GROUP BY f_chr, f_pos, f_alt, geo_region_name_en, f_region_total_samples
) dummy
LIMIT 10
```
               


## Times


|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet | Time (s) - Parquet - gzip |
|-----      |--------  |------            |------              |------              |
| raw       | Tez      | 376(241)(258s)   | 1537               |                |
| raw       | Presto   |   2400(1961)     |    >1h             |                 |
| raw       | SparkSQL |        624(672)  |   576(552)         |            |
| raw       | MonetDB |        2595       |                    |                    |
|aggr       | Presto   |      59(39)      | 44                 |   38               |
|aggr       | SparkSQL |   21(33)         | 13                 |                  |
|aggr       | Tez      | 63(94)           | 58                 |  58              |
|aggr       | MonetDB      |    207       |  207               |                 |
|aggr+denom | Tez      | 63(70)           | 54                 |  58              |
|aggr+denom | SparkSQL |    21(36)        | 13                 | 13                 |
|aggr+denom | Presto   |     41(43)       | 44                 | 46                 |
|aggr+denom | MonetDB  |     182/178      |                    |                    |
|aggr+denom | Kylin  |     0.32           |                    |                    |


# Query 2

Allele frequencies - breakdown by countries

## SQL

###raw:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val) as var_cnt
     , geo_sample_cnt
    --  , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_sample_id
                    , f_geo_id
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_new_orc_partitioned) fact_new_orc_partitioned
JOIN dim_geography ON f_geo_id = geo_id 
JOIN (
      SELECT  geo_country_name_en as grn
            , COUNT(DISTINCT f_sample_id) as geo_sample_cnt
      FROM fact_new_orc_partitioned JOIN dim_geography ON f_geo_id = geo_id
      GROUP BY geo_country_name_en) geo_sample_cnt 
ON geo_country_name_en = grn
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, geo_sample_cnt
LIMIT 10
) dummy
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (cast (var_cnt as double) / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val) as var_cnt
     , geo_sample_cnt
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_sample_id
                    , f_geo_id
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_new_orc_partitioned) fact_new_orc_partitioned
JOIN dim_geography ON f_geo_id = geo_id 
JOIN (
      SELECT  geo_country_name_en as grn
            , COUNT(DISTINCT f_sample_id) as geo_sample_cnt
      FROM fact_new_orc_partitioned JOIN dim_geography ON f_geo_id = geo_id
      GROUP BY geo_country_name_en) geo_sample_cnt 
ON geo_country_name_en = grn
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, geo_sample_cnt
LIMIT 10
) dummy
```

aggr:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_country_total_samples as geo_sample_cnt
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_geo_id
                    , f_d_id
                    , f_country_total_samples
                    , f_cnt_alter
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_agg_genotypes_counts) fact_orc_optimized2
JOIN dim_geography ON f_geo_id = geo_id
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, f_country_total_samples
LIMIT 10
) dummy
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (cast (var_cnt as double) / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_country_total_samples as geo_sample_cnt
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_geo_id
                    , f_d_id
                    , f_country_total_samples
                    , f_cnt_alter
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_agg_genotypes_counts) fact_orc_optimized2
JOIN dim_geography ON f_geo_id = geo_id
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, f_country_total_samples
LIMIT 10
) dummy
```


##aggr+denorm:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_country_total_samples as geo_sample_cnt
    --  , (var_cnt / (2 * geo_sample_cnt)) as var_frequency
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_geo_id
                    , f_d_id
                    , f_country_total_samples
                    , geo_country_name_en
                    , f_cnt_alter
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_agg_genotypes_counts_dims) fact_orc_optimized2 
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, f_country_total_samples
LIMIT 10
) dummy
```

Presto:

```sql
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , var_cnt
     , geo_sample_cnt
     , (cast (var_cnt as double) / (2 * geo_sample_cnt)) as var_frequency
FROM
(
SELECT f_chr
     , f_pos
     , f_alt
     , geo_country_name_en
     , SUM(genotype_val * f_cnt_alter) as var_cnt
     , f_country_total_samples as geo_sample_cnt
FROM (SELECT DISTINCT f_chr
                    , f_pos
                    , f_alt
                    , f_geo_id
                    , f_d_id
                    , f_country_total_samples
                    , geo_country_name_en
                    , f_cnt_alter
                    , if(f_genotype='1/1',2,1) as genotype_val 
      FROM fact_agg_genotypes_counts_dims) fact_orc_optimized2 
GROUP BY f_chr, f_pos, f_alt, geo_country_name_en, f_country_total_samples
LIMIT 10
) dummy
```



## Times

|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------  |------            |------              |
| raw       | Tez      |  732(250)(269s)  |    894             |
| raw       | Presto   |       >1h        |       >1h          |
| raw       | SparkSQL |    236(690)      |  213(176)          |
|aggr       | Presto   |    81(64)        |   66               |
|aggr       | SparkSQL |   18()           | 13                 |
|aggr       | Tez      | 80(77)           | 57                 |
|aggr | Monetdb  |    44              |       44         |
|aggr+denom | Tez      | 64(88)           |  61                |
|aggr+denom | SparkSQL |   21(74)         |   14               |
|aggr+denom | Presto   |   43(49)         |   43               |
|aggr+denom | Monetdb  |      143(!-bug)            |                    |
| cube      | Kylin               | Kylin_1  |0.85             |                    |

# Query 3

##raw

```sql
SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , cast(SUM(if(f_genotype='1/1',2,1)) as double) / (2*s_cnt) as var_frequency
               , f_ensembl_gp_id
          FROM fact_new_pq
          LEFT JOIN (
                    SELECT  COUNT(DISTINCT f_sample_id) as s_cnt 
                    FROM fact_new_pq) sam_cnt  ON 1 = 1
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_v_id, s_cnt, f_ensembl_gp_id) var_freq 
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON v_id=f_v_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D'
GROUP BY gp_trans_id
LIMIT 10;
```

##aggr:

```sql

SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , (cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples)) as var_frequency
               , f_ensembl_gp_id
          FROM fact_agg_genotypes_counts_pq
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_ensembl_gp_id, f_v_id, f_total_samples
          ) var_freq
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON f_v_id = v_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D'
GROUP BY gp_trans_id
LIMIT 10;

```


aggr+denorm:

```sql
SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM (
SELECT f_chr
     , f_pos
     , f_alt
     , f_variant_name
     , gp_trans_id
     , cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples) as var_frequency
FROM fact_agg_genotypes_counts_dims_orc
WHERE v_fathmm_pred = 'D' and f_ensembl_gp_id IS NOT NULL
GROUP BY f_chr, f_pos, f_alt, f_variant_name, gp_trans_id, f_total_samples
) t
WHERE var_frequency <= 0.01
GROUP BY gp_trans_id
LIMIT 10;
```


## Times
|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------  |------            |------              |
| raw       | Tez      |   201(192)       |  332               |
| raw       | Presto   |    >1h           |     >1h            |
| raw       | SparkSQL |    548           |  473               |
|aggr       | Presto   |   59(53)         |   36               |
|aggr       | SparkSQL |    250(326)      |  261               |
|aggr       | Tez      |    119(178)      |  122               |
|aggr       | MonetDB  |       263        |                    |
|aggr+denom | Tez      |   47(56)         |  36                |
|aggr+denom | SparkSQL |  15(70)          |   9                |
|aggr+denom | Presto   |  10(19)          |   8                |
|aggr+denom | Monetdb  |  18/15           |                    |
| cube      | Kylin    |1.7               | 1.7                |

# Query 4

##raw
```sql
SELECT gp_trans_id
     , gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM( SELECT f_chr
       , f_pos
       , f_alt
       , f_variant_name
       , cast(SUM(if(f_genotype='1/1',2,1)) as double) / (2*s_cnt) as var_frequency
       , gp_trans_id
       , gp_exon_num
       , f_v_id
  FROM fact_new_orc
  LEFT JOIN (SELECT COUNT(DISTINCT f_sample_id) as s_cnt FROM fact_new_orc) sam_cnt  ON 1 = 1
  LEFT JOIN dim_genomic_position_ensembl ON f_ensembl_gp_id=gp_id
  WHERE f_ensembl_gp_id IS NOT NULL
  GROUP BY f_chr, f_pos, f_alt, f_variant_name, gp_trans_id, gp_exon_num, s_cnt, f_v_id
  ) var_freq 
LEFT JOIN dim_variant_predictions ON v_id=f_v_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D'
GROUP BY gp_trans_id, gp_exon_num
LIMIT 10;
```

##aggr:

```sql
SELECT gp_trans_id
     , gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , (cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples)) as var_frequency
               , f_ensembl_gp_id
          FROM fact_agg_genotypes_counts_pq
          JOIN dim_genomic_position_ensembl ON f_ensembl_gp_id = gp_id
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_ensembl_gp_id, gp_exon_num, f_v_id, f_total_samples
          ) var_freq
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON f_v_id = v_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D'
GROUP BY gp_trans_id
LIMIT 10;
```




aggr+denorm:

```sql
SELECT gp_trans_id
     , gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM (
SELECT f_chr
     , f_pos
     , f_alt
     , f_variant_name
     , gp_trans_id
     , gp_exon_num
     , cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples) as var_frequency
FROM fact_agg_genotypes_counts_dims_orc
WHERE v_fathmm_pred = 'D' and f_ensembl_gp_id IS NOT NULL
GROUP BY f_chr, f_pos, f_alt, f_variant_name, gp_trans_id, gp_exon_num, f_total_samples
) t
WHERE var_frequency <= 0.01
GROUP BY gp_trans_id, gp_exon_num
LIMIT 10;
```

## Times

|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------  |------            |------              |
| raw       | Tez      |    237(258)      |    459             |
| raw       | Presto   |    >1h           |     >1h            |
| raw       | SparkSQL |   840            |    720             |
|aggr       | Presto   |  157(150)        |     150            |
|aggr       | SparkSQL |   292(361)       |    251             |
|aggr       | Tez      |    139(123)      |     142            |
|aggr       | Monet      |    321      |                |
|aggr+denom | Tez      |     58(56)       |      35            |
|aggr+denom | SparkSQL |    15(153)       |     9              |
|aggr+denom | Presto   |   12(23)         |     9              |
|aggr+denom | Monetdb  |   17/17               |                    |
| cube      | Kylin    |2.5              |            |

# Query 5

##raw

```sql
SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , cast(SUM(if(f_genotype='1/1',2,1)) as double) / (2*s_cnt) as var_frequency
               , f_ensembl_gp_id, f_d_id
          FROM fact_new_pq
          LEFT JOIN (
                    SELECT  COUNT(DISTINCT f_sample_id) as s_cnt 
                    FROM fact_new_pq) sam_cnt  ON 1 = 1
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_v_id, s_cnt, f_ensembl_gp_id, f_d_id) var_freq 
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON v_id=f_v_id
LEFT JOIN dim_disease ON d_id=f_d_id
WHERE var_frequency <= 0.01 AND v_fathmm_pred = 'D' AND  d_omim_id='OMIM:308230'
GROUP BY gp_trans_id
LIMIT 10;
```


##aggr:

```sql

SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , (cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples)) as var_frequency
               , f_ensembl_gp_id, f_d_id
          FROM fact_agg_genotypes_counts_pq
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_ensembl_gp_id, f_v_id, f_total_samples, f_d_id
          ) var_freq
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON f_v_id = v_id
LEFT JOIN dim_disease ON d_id=f_d_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D' AND d_omim_id='OMIM:308230'
GROUP BY gp_trans_id
LIMIT 10;

```


##aggr+denorm:

```sql
SELECT gp_trans_id
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM (
SELECT f_chr
     , f_pos
     , f_alt
     , f_variant_name
     , gp_trans_id
     , cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples) as var_frequency
FROM fact_agg_genotypes_counts_dims_orc
WHERE v_fathmm_pred = 'D' and f_ensembl_gp_id IS NOT NULL AND d_omim_id='OMIM:308230'
GROUP BY f_chr, f_pos, f_alt, f_variant_name, gp_trans_id, f_total_samples
) t
WHERE var_frequency <= 0.01 
GROUP BY gp_trans_id
LIMIT 10;
```




|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------  |------            |------              |
| raw       | Tez      |     217(226)     |    359             |
| raw       | Presto   |         > 1h     |      > 1h           |
| raw       | SparkSQL |      509         |    446             |
| raw       | Monet    |        failed-No space left on device          |                    |
|aggr       | Presto   |    118(126)      |    146             |
|aggr       | SparkSQL |     261(280)     |    234             |
|aggr       | Tez      |  134(311)        |    157             |
|aggr       | Monet    | 549              |                    |
|aggr+denom | Tez      |   34(48)         |       21           |
|aggr+denom | SparkSQL |   14(51)         |       9           |
|aggr+denom | Presto   |    62(81)        |       19           |
|aggr+denom | Monetdb  |   0.5            |                    |
| cube      | Kylin    |      13          |                    |


# Query 6

##raw

```sql
SELECT gp_trans_id,
gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , cast(SUM(if(f_genotype='1/1',2,1)) as double) / (2*s_cnt) as var_frequency
               , f_ensembl_gp_id, f_d_id
          FROM fact_new_pq
          LEFT JOIN (
                    SELECT  COUNT(DISTINCT f_sample_id) as s_cnt 
                    FROM fact_new_pq) sam_cnt  ON 1 = 1
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_v_id, s_cnt, f_ensembl_gp_id, f_d_id) var_freq 
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON v_id=f_v_id
LEFT JOIN dim_disease ON d_id=f_d_id
WHERE var_frequency <= 0.01 AND v_fathmm_pred = 'D' AND  d_omim_id='OMIM:308230'
GROUP BY gp_trans_id,gp_exon_num
LIMIT 10;
```


##aggr:

```sql

SELECT gp_trans_id,
gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM dim_genomic_position_ensembl
LEFT JOIN (
          SELECT f_chr
               , f_pos
               , f_alt
               , f_v_id
               , f_variant_name
               , (cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples)) as var_frequency
               , f_ensembl_gp_id, f_d_id
          FROM fact_agg_genotypes_counts_pq
          WHERE f_ensembl_gp_id IS NOT NULL
          GROUP BY f_chr, f_pos, f_alt, f_variant_name, f_ensembl_gp_id, f_v_id, f_total_samples, f_d_id
          ) var_freq
ON f_ensembl_gp_id=gp_id
LEFT JOIN dim_variant_predictions ON f_v_id = v_id
LEFT JOIN dim_disease ON d_id=f_d_id
WHERE var_frequency <= 0.01 and v_fathmm_pred = 'D' AND d_omim_id='OMIM:308230'
GROUP BY gp_trans_id,gp_exon_num
LIMIT 10;

```


##aggr+denorm:

```sql
SELECT gp_trans_id,
gp_exon_num
     , COUNT(DISTINCT f_variant_name) as distinct_variant_cnt
     , SUM(var_frequency) as cum_freq
FROM (
SELECT f_chr
     , f_pos
     , f_alt
     , f_variant_name
     , gp_trans_id,
    gp_exon_num
     , cast(SUM(if(f_genotype='1/1',2,1) * f_cnt_alter) as double) / (2 * f_total_samples) as var_frequency
FROM fact_agg_genotypes_counts_dims_orc
WHERE v_fathmm_pred = 'D' and f_ensembl_gp_id IS NOT NULL
GROUP BY f_chr, f_pos, f_alt, f_variant_name, gp_trans_id,gp_exon_num, f_total_samples
) t
WHERE var_frequency <= 0.01 AND d_omim_id='OMIM:308230'
GROUP BY gp_trans_id,gp_exon_num
LIMIT 10;
```


|Level      | Engine   | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------  |------            |------              |
| raw       | Tez      |      216(230)    |      359           |
| raw       | Presto   | >1h              |       >1h          |
| raw       | SparkSQL |      900        |       720           |
| raw       | Monet |           failed - No space left on device     |                    |
|aggr       | Presto   |     120(128)      |     113            |
|aggr       | SparkSQL |     264(282)     |     225            |
|aggr       | Tez      |     140(300)     |     173            |
|aggr       | Monet    |       549        |                    |
|aggr+denom | Tez      |    49(60)        |      33            | 
|aggr+denom | SparkSQL |   17(53)         |        8           |
|aggr+denom | Presto   |    15(32)        |        9           |
|aggr+denom | Monetdb  |     0.3          |                    |
| cube      | Kylin    |     14           |                    |


# Query 7

Distribution of variant’s depth of coverage in transcript across samples

## SQL

###raw:

```sql
SELECT
      gp_trans_id
    , min(avg_total_depth) as min
    , percentile_approx(avg_total_depth, 0.25) as 25_percentile
    , percentile_approx(avg_total_depth, 0.5) as median
    , percentile_approx(avg_total_depth, 0.75) as 75_percentile
    , max(avg_total_depth) as max
FROM
  (SELECT
    avg(f_total_depth) as avg_total_depth
  , f_chr
  , f_pos
  , f_ensembl_gp_id
  FROM fact_new_pq
  GROUP BY f_chr, f_pos, f_ensembl_gp_id) facts
RIGHT JOIN dim_genomic_position_ensembl ON f_ensembl_gp_id = gp_id
GROUP BY gp_trans_id
ORDER BY gp_trans_id
```


### aggr:

```sql
SELECT gp_trans_id, 
       Min(avg_total_depth)                     AS min, 
       Percentile_approx(avg_total_depth, 0.25) AS 25_percentile, 
       Percentile_approx(avg_total_depth, 0.5)  AS median, 
       Percentile_approx(avg_total_depth, 0.75) AS 75_percentile, 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id 
        FROM   dwh.fact_agg_counts 
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id) facts 
       RIGHT JOIN dim_genomic_position_ensembl 
               ON f_ensembl_gp_id = gp_id 
GROUP  BY gp_trans_id
ORDER  BY gp_trans_id; 
```

### aggr+denormalizacja:

```sql
SELECT gp_trans_id, 
       Min(avg_total_depth)                     AS min, 
       Percentile_approx(avg_total_depth, 0.25) AS 25_percentile, 
       Percentile_approx(avg_total_depth, 0.5)  AS median, 
       Percentile_approx(avg_total_depth, 0.75) AS 75_percentile, 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id 
        FROM   dwh.fact_agg_counts_dims
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id) facts 
       RIGHT JOIN dim_genomic_position_ensembl 
               ON f_ensembl_gp_id = gp_id  
GROUP  BY gp_trans_id
ORDER  BY gp_trans_id; 
```              


## Times

|Level      | Engine    | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------   |------            |------              |
| raw       | Tez       |     110(170)     |      157           |
| raw       | Presto    |    132(109)      |     122            |
| raw       | SparkSQL  |      80          |      44            |
|aggr       | Presto    |     69(43)       |      47            |
|aggr       | SparkSQL  |    20(49)        |     21             |
|aggr       | Tez       |    69(117)       |     70             |
|aggr       | Monetdb   |146/148           |                    | 
|aggr+denom | Tez       |    100(122)      |     61             |
|aggr+denom | SparkSQL  |    20(40)        |     17             |
|aggr+denom | Presto    |    42(48)        |     43             |
|aggr+denom | Monetdb   | 91/90            |                    |
| cube      | Kylin     |      0.26            |                    |

# Query 8

Distribution of variant’s depth of coverage in exon across samples

## SQL

###raw:

```sql
SELECT
      gp_trans_id
    , gp_exon_num
    , min(avg_total_depth) as min
    , percentile_approx(avg_total_depth, 0.25) as 25_percentile
    , percentile_approx(avg_total_depth, 0.5) as median
    , percentile_approx(avg_total_depth, 0.75) as 75_percentile
    , max(avg_total_depth) as max
FROM
  (SELECT
    avg(f_total_depth) as avg_total_depth
  , f_chr
  , f_pos
  , f_ensembl_gp_id
  FROM fact_new_pq
  GROUP BY f_chr, f_pos, f_ensembl_gp_id) facts
RIGHT JOIN dim_genomic_position_ensembl ON f_ensembl_gp_id = gp_id
WHERE gp_trans_id = 'ENST00000424598'
GROUP BY gp_trans_id, gp_exon_num
ORDER BY gp_exon_num
```



Presto:

```sql
SELECT
      gp_trans_id
    , gp_exon_num
    , min(avg_total_depth) as min
    , Approx_percentile(avg_total_depth, 0.25) as "25_percentile"
    , Approx_percentile(avg_total_depth, 0.5) as median
    , Approx_percentile(avg_total_depth, 0.75) as "75_percentile"
    , max(avg_total_depth) as max
FROM
  (SELECT
    avg(f_total_depth) as avg_total_depth
  , f_chr
  , f_pos
  , f_ensembl_gp_id
  FROM fact_new_pq
  GROUP BY f_chr, f_pos, f_ensembl_gp_id) facts
RIGHT JOIN dim_genomic_position_ensembl ON f_ensembl_gp_id = gp_id
WHERE gp_trans_id = 'ENST00000424598'
GROUP BY gp_trans_id, gp_exon_num
ORDER BY gp_exon_num
```

### aggr:

```sql
SELECT gp_trans_id, 
       gp_exon_num, 
       Min(avg_total_depth)                     AS min, 
       Percentile_approx(avg_total_depth, 0.25) AS 25_percentile, 
       Percentile_approx(avg_total_depth, 0.5)  AS median, 
       Percentile_approx(avg_total_depth, 0.75) AS 75_percentile, 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id 
        FROM   dwh.fact_agg_genotypes_counts_orc_partitioned
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id) facts 
       RIGHT JOIN dim_genomic_position_ensembl 
               ON f_ensembl_gp_id = gp_id 
WHERE  gp_trans_id = 'ENST00000424598' 
GROUP  BY gp_trans_id, 
          gp_exon_num 
ORDER  BY gp_exon_num; 
```

Presto:

```sql
SELECT gp_trans_id, 
       gp_exon_num, 
       Min(avg_total_depth)                     AS min, 
       Approx_percentile(avg_total_depth, 0.25) AS "25_percentile", 
       Approx_percentile(avg_total_depth, 0.5)  AS median, 
       Approx_percentile(avg_total_depth, 0.75) AS "75_percentile", 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id 
        FROM   dwh.fact_agg_genotypes_counts_pq
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id) facts 
       RIGHT JOIN dim_genomic_position_ensembl 
               ON f_ensembl_gp_id = gp_id 
WHERE  gp_trans_id = 'ENST00000424598' 
GROUP  BY gp_trans_id, 
          gp_exon_num 
ORDER  BY gp_exon_num; 
```



### aggr+denormalizacja:

```sql
SELECT gp_trans_id, 
       gp_exon_num, 
       Min(avg_total_depth)                     AS min, 
       Percentile_approx(avg_total_depth, 0.25) AS 25_percentile, 
       Percentile_approx(avg_total_depth, 0.5)  AS median, 
       Percentile_approx(avg_total_depth, 0.75) AS 75_percentile, 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id,
               gp_trans_id,
               gp_exon_num
        FROM   dwh.fact_agg_genotypes_counts_dims_pq
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id,
                  gp_trans_id,
                  gp_exon_num) facts 
WHERE  gp_trans_id = 'ENST00000424598' 
GROUP  BY gp_trans_id, 
          gp_exon_num 
ORDER  BY gp_exon_num; 
```

Presto:

```sql
SELECT gp_trans_id, 
       gp_exon_num, 
       Min(avg_total_depth)                     AS min, 
       Approx_percentile(avg_total_depth, 0.25) AS "25_percentile", 
       Approx_percentile(avg_total_depth, 0.5)  AS median, 
       Approx_percentile(avg_total_depth, 0.75) AS "75_percentile", 
       Max(avg_total_depth)                     AS max 
FROM   (SELECT SUM(f_sum_total_depth) / SUM(f_cnt_alter) AS avg_total_depth, 
               f_chr, 
               f_pos, 
               f_ensembl_gp_id,
               gp_trans_id,
               gp_exon_num               
        FROM   dwh.fact_agg_genotypes_counts_dims_pq_gzip
        GROUP  BY f_chr, 
                  f_pos, 
                  f_ensembl_gp_id,
                  gp_trans_id,
                  gp_exon_num) facts 
WHERE  gp_trans_id = 'ENST00000424598' 
GROUP  BY gp_trans_id, 
          gp_exon_num 
ORDER  BY gp_exon_num; 
```


## Times

|Level      | Engine              | Time (s) - ORC   | Time (s) - Parquet |
|-----      |--------             |------            |------              |
| raw       | Tez                 |     96(123)      |         115        |
| raw       | SparkSQL            |      81          |     39             |
| raw       | Presto              |      81          |     79             |
|aggr       | Presto              |     43(18)       |     12             |
|aggr       | SparkSQL            |     18(45)       |     14             |
|aggr       | Tez                 |    52(101)         |   53               |
|aggr       | Monetdb-cold/warm   | 157/149          |                    |
|aggr+denom | Tez                 |    33(48)        |     28             |
|aggr+denom | SparkSQL            | 26(38)           |     8             |
|aggr+denom | Presto              | 7(14)           |     6             |
|aggr+denom | Monetdb-cold/warm   |3.0/0.5            |                    |
| cube      | Kylin               |0.91              |                    |