## Enrichment script refactoring

We have refactored the enrichment calculation step so that it runs in ~3 minutes with minimal RAM requirements.

Only supports using a specified set of SNPs (no LD calcaulation support).

### Setup

```
# Install java 8 (for spark) e.g. using apt
sudo apt install -yf openjdk-8-jre-headless openjdk-8-jdk

# Install dependencies into isolated environment
conda env create -n cheers_otg --file environment.yaml

# Activate environment, set RAM availability
conda activate cheers_otg
export PYSPARK_SUBMIT_ARGS="--driver-memory 50g pyspark-shell"
```

### Usage

```
$ python OTG_computeEnrichment.py --help
usage: OTG_computeEnrichment.py [-h] --in_peaks <str> --in_snps <str>
                                --out_stats <str> [--out_log <str>]
                                [--out_unique_peaks <str>]
                                [--out_snp_peak_overlaps <str>]
                                [--cores <int>]

optional arguments:
  -h, --help            show this help message and exit
  --in_peaks <str>      Input peaks
  --in_snps <str>       Input SNPs
  --out_stats <str>     Output statistics
  --out_log <str>       Output log file
  --out_unique_peaks <str>
                        Output peaks that have >=1 overlapping SNP
  --out_snp_peak_overlaps <str>
                        Output SNPs and their overlapping peaks
  --cores <int>         Number of cores to use. Set to -1 to use all (default:
                        -1)
```

### Examples

```bash
# Minimal set of arguments (recommended)
python OTG_computeEnrichment.py \
  --in_peaks example_data/DNase_seq_Roadmap_counts_normToMax_quantileNorm_euclideanNorm.head10k.txt \
  --in_snps example_data/GCST004131.txt \
  --out_stats output/traitA_enrichmentStats.tsv

# Full set of args. Will require additional computation to make the required outputs.
python OTG_computeEnrichment.py \
  --in_peaks example_data/DNase_seq_Roadmap_counts_normToMax_quantileNorm_euclideanNorm.head10k.txt \
  --in_snps example_data/GCST004131.txt \
  --out_stats output/traitA_enrichmentStats.tsv \
  --out_log output/traitA_log.txt \
  --out_unique_peaks output/traitA_uniquePeaks.tsv \
  --out_snp_peak_overlaps output/traitA_snpPeakOverlaps.tsv \
  --cores 4
```

### Input data formats

`--in_peaks` should be a TSV (with header) where first 3 columns are chr, start, end and rest of columns are the sample scores. E.g.

```
chr     start   end     ENCFF521LXH     ENCFF098TNV     ENCFF456KNS
chr10   132175555       132175705       0.1325  0.0413  0.1102
chr13   100721555       100721705       0.0932  0.0636  0.3428
chr16   56067515        56067705        0.0216  0.072   0.0214
chr17   47994795        47994947        0.1067  0.014   0.5189
chr3    106408915       106409165       0.0697  0.1555  0.0914
...
```

`--in_snps` should be a TSV (without header) where columns are variant ID, chromosome, position. Chromosome names must match those used in peak file.

```
1_150709618_C_T chr1    150709618
1_172895043_G_A chr1    172895043
10_35039427_GT_G        chr10   35039427
12_40414428_A_G chr12   40414428
14_68813162_A_G chr14   68813162
16_28518628_T_C chr16   28518628
...
```
