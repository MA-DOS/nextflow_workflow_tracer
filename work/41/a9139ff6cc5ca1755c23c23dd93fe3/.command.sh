#!/bin/bash -euo pipefail
samtools \
    idxstats \
    --threads 0 \
    OSMOTIC_STRESS_T0_PE_REP1_T1.Lb.sorted.bam \
    > OSMOTIC_STRESS_T0_PE_REP1_T1.Lb.sorted.bam.idxstats

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_ALIGN_BWA:BAM_SORT_STATS_SAMTOOLS:BAM_STATS_SAMTOOLS:SAMTOOLS_IDXSTATS":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
