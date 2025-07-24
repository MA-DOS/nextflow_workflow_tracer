#!/bin/bash -euo pipefail
samtools \
    flagstat \
    --threads 1 \
    OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam \
    > OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam.flagstat

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_FILTER_BAM:BAM_STATS_SAMTOOLS:SAMTOOLS_FLAGSTAT":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
