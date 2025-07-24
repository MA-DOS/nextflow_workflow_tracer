#!/bin/bash -euo pipefail
samtools \
    flagstat \
    --threads 1 \
    OSMOTIC_STRESS_T0_PE_REP2.mLb.mkD.sorted.bam \
    > OSMOTIC_STRESS_T0_PE_REP2.mLb.mkD.sorted.bam.flagstat

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_MARKDUPLICATES_PICARD:BAM_STATS_SAMTOOLS:SAMTOOLS_FLAGSTAT":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
