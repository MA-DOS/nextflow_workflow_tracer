#!/bin/bash -euo pipefail
samtools \
    flagstat \
    --threads 1 \
    OSMOTIC_STRESS_T150_SE_REP1_T2.Lb.sorted.bam \
    > OSMOTIC_STRESS_T150_SE_REP1_T2.Lb.sorted.bam.flagstat

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_ALIGN_BWA:BAM_SORT_STATS_SAMTOOLS:BAM_STATS_SAMTOOLS:SAMTOOLS_FLAGSTAT":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
