#!/bin/bash -euo pipefail
samtools \
    stats \
    --threads 1 \
    --reference genome.fa \
    OSMOTIC_STRESS_T100_SE.mRp.clN.sorted.bam \
    > OSMOTIC_STRESS_T100_SE.mRp.clN.sorted.bam.stats

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_MARKDUPLICATES_PICARD:BAM_STATS_SAMTOOLS:SAMTOOLS_STATS":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
