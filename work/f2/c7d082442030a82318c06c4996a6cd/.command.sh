#!/bin/bash -euo pipefail
samtools \
    stats \
    --threads 1 \
    --reference genome.fa \
    OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.sorted.bam \
    > OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.sorted.bam.stats

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_FILTER_BAM:BAM_SORT_STATS_SAMTOOLS:BAM_STATS_SAMTOOLS:SAMTOOLS_STATS":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
