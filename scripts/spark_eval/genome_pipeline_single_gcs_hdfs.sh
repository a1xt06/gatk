#!/usr/bin/env bash

# Run the pipeline (ReadsPipelineSpark) on genome data on a GCS Dataproc cluster. Data is in HDFS.

. utils.sh

time_gatk "ReadsPipelineSpark -I hdfs:///user/$USER/q4_spark_eval/WGS-G94982-NA12878.bam -O hdfs://${GCS_CLUSTER}-m:8020/user/$USER/q4_spark_eval/out/WGS-G94982-NA12878.vcf -R hdfs:///user/$USER/q4_spark_eval/human_g1k_v37.2bit --knownSites hdfs://${GCS_CLUSTER}-m:8020/user/$USER/q4_spark_eval/dbsnp_138.b37.vcf -XL NC_007605 -pairHMM AVX_LOGLESS_CACHING -maxReadsPerAlignmentStart 10" 10 8 42g 4g