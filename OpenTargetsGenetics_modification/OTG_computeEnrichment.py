#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Refactoring of CHEERS enrichment script
#

# Standard
import argparse
import os
import sys
from typing import Iterable
import math
from functools import partial
import time
# Other
from scipy.stats import norm
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main():

    start_time = time.time()

    # Parse args
    args = parse_args()

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        # Set the number of cores to use (only works in spark's local mode)
        .config("spark.master", "local[{}]".format(
            '*' if args.cores == -1 else args.cores))
        # Increase the number of port retries
        .config('spark.port.maxRetries', 512)
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    #
    # Load datasets -----------------------------------------------------------
    #

    peaks_wide = load_peaks(args.in_peaks).persist()
    snps = load_snps(args.in_snps)

    #
    # Join snps to peaks ------------------------------------------------------
    #

    # Use semi join to keep rows for peaks that overlap snps
    peaks_overlapping = (
        # Only need the peak coords
        peaks_wide
        .select('chr', 'start', 'end')
        # Do a broadcast non-equi join
        .join(
            broadcast(snps),
            ((col('chr_snp') == col('chr')) &
             (col('pos') >= col('start')) &
             (col('pos') <= col('end'))
            ), how='left_semi')
        # Drop chr_snp as duplicated
        .drop('chr_snp')
        # Persist as we need to count later
        .persist()
    )

    # Store total number of peaks and total overlapping peaks
    n_total_peaks = float(peaks_wide.count())
    n_unique_peaks = float(peaks_overlapping.count())

    # Join these back to the full list of peaks. So that we know which
    # peaks have overlaps
    peaks_wide = (
        # Add column showing that the park has an overlap
        peaks_overlapping
        .withColumn('has_overlap', lit(True))
        # Join these back to the full peaks
        .join(peaks_wide, on=['chr', 'start', 'end'], how='right')
        # Add false when a peak hasn't got an overlap
        .withColumn('has_overlap',
            when(col('has_overlap').isNull(), lit(False)).otherwise(
                lit(True)
            )
        )
    )

    #
    # Melt data to long format ------------------------------------------------
    #

    # Melt peaks to long format
    sample_names = peaks_wide.columns[4:]
    peaks_long = melt(
        df=peaks_wide,
        id_vars=['chr', 'start', 'end', 'has_overlap'],
        value_vars=sample_names,
        var_name='sample',
        value_name='score'
    )

    #
    # Calculate peak ranks ----------------------------------------------------
    #

    # Create window specification to rank over
    window_spec = (
        Window
        .partitionBy('sample')
        .orderBy('score')
    )

    # Get ranks. The original code starts ranks from 0, so subtract 1.
    peak_ranks = peaks_long.withColumn('rank', rank().over(window_spec) - 1)

    #
    # Calculate enrichments ---------------------------------------------------
    #

    # Get unique peaks that overlap >=1 SNP (drop peaks that have no overlaps)
    unique_peaks = (
        peak_ranks
        .filter(col('has_overlap'))
    )

    # Get mean rank per sample
    sample_mean_rank = (
        unique_peaks
        .groupby('sample')
        .agg(
            mean(col('rank')).alias('mean_rank')
        )
        .cache()
    )

    # Define parameters for descrete uniform distribution and calculate p-value
    mean_sd = math.sqrt(((n_total_peaks**2) - 1) / (12 * n_unique_peaks))
    mean_mean = (1 + n_total_peaks) / 2

    # Make partial function to caluclate norm.cdf
    norm_cdf_partial = partial(norm.cdf, loc=mean_mean, scale=mean_sd)

    # Make into a udf
    norm_cdf_udf = udf(lambda x: float(norm_cdf_partial(x)), DoubleType())

    # Calculate p-value enrichments
    enrichment_pvals = (
        sample_mean_rank
        .withColumn('pvalue', 1 - norm_cdf_udf(col('mean_rank')))
    )

    #
    # Write outputs -----------------------------------------------------------
    #

    # Write enrichment statsitics (required)
    os.makedirs(os.path.dirname(args.out_stats), exist_ok=True)
    (
        enrichment_pvals
        .toPandas()
        .to_csv(
            args.out_stats,
            sep='\t',
            index=None
        )
    )

    # Write a table of unique peaks (peaks that have >= 1 overlapping SNP)
    if args.out_unique_peaks:
        os.makedirs(os.path.dirname(args.out_unique_peaks), exist_ok=True)
        (
            unique_peaks
            .drop('has_overlap')
            .toPandas()
            .to_csv(
                args.out_unique_peaks,
                sep='\t',
                index=None
            )
        )
    
    # Write a table SNPs and their overlapping peaks
    # This will perform the join as it is not required above for calculating
    # enrichment statistics, therefore will be compute intensive!
    if args.out_snp_peak_overlaps:

        # Calculate overlap
        snp_peaks = (
            # Only need the peak coords
            peaks_wide
            .select('chr', 'start', 'end')
            # Do a broadcast non-equi join
            .join(
                broadcast(snps),
                ((col('chr_snp') == col('chr')) &
                (col('pos') >= col('start')) &
                (col('pos') <= col('end'))
                ), how='inner')
            # Drop chr_snp as duplicated
            .drop('chr_snp')
        )
        
        # Write
        os.makedirs(os.path.dirname(args.out_snp_peak_overlaps), exist_ok=True)
        (
            snp_peaks
            .toPandas()
            .to_csv(
                args.out_snp_peak_overlaps,
                sep='\t',
                index=None
            )
        )

    # Write log
    if args.out_log:
        os.makedirs(os.path.dirname(args.out_log), exist_ok=True)
        with open(args.out_log, 'w') as out_h:
            out_h.write('Total number of peaks:\t{}\n'.format(n_total_peaks))
            out_h.write('Number of overlapping peaks:\t{}\n'.format(n_unique_peaks))
            out_h.write('Distribution mean:\t{}\n'.format(mean_mean))
            out_h.write('Distribution sd:\t{}\n'.format(mean_sd))
            out_h.write('Running time (seconds):\t{}\n'.format(time.time() - start_time))
    
    return 0

def load_snps(in_path):
    ''' Loads snp list for enrichment
    Params:
        in_path (file)
    Returns:
        spark.df
    '''
    # Load SNPs
    import_schema = StructType([
        StructField("varid", StringType(), False),
        StructField("chr_snp", StringType(), False),
        StructField("pos", IntegerType(), False)
    ])
    snps = (
        spark.read.csv(in_path, sep='\t', header=False, schema=import_schema)
        .repartitionByRange('chr_snp', 'pos')
    )
    return snps

def load_peaks(in_path):
    ''' Loads peak coords and scores to wide df
    Params:
        in_path (file)
    Returns:
        spark.df
    '''
    # Load peaks. Can't specify schema due to variable column names.
    peaks_wide = (
        spark.read.csv(in_path, sep='\t', header=True, inferSchema=True)
        .repartitionByRange('chr', 'start', 'end')
    )

    return peaks_wide


def melt(df: pyspark.sql.DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> pyspark.sql.DataFrame:
    """Convert :class:`DataFrame` from wide to long format.
    Copied from: https://stackoverflow.com/a/41673644
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_peaks', metavar="<str>", help="Input peaks", type=str, required=True)
    parser.add_argument('--in_snps', metavar="<str>", help="Input SNPs", type=str, required=True)
    parser.add_argument('--out_stats', metavar="<str>", help="Output statistics", type=str, required=True)
    parser.add_argument('--out_log', metavar="<str>", help="Output log file", type=str, required=False)
    parser.add_argument('--out_unique_peaks', metavar="<str>", help="Output peaks that have >=1 overlapping SNP", type=str, required=False)
    parser.add_argument('--out_snp_peak_overlaps', metavar="<str>", help="Output SNPs and their overlapping peaks", type=str, required=False)
    parser.add_argument('--cores', metavar="<int>", help="Number of cores to use. Set to -1 to use all (default: -1)", type=int, default=-1)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
