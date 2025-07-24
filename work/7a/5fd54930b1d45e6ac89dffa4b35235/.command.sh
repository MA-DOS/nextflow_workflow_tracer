#!/bin/bash -euo pipefail
samtools \
    stats \
    --threads 1 \
    --reference genome.fa \
    OSMOTIC_STRESS_T0_PE_REP2_T1.Lb.sorted.bam \
    > OSMOTIC_STRESS_T0_PE_REP2_T1.Lb.sorted.bam.stats

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_ALIGN_BWA:BAM_SORT_STATS_SAMTOOLS:BAM_STATS_SAMTOOLS:SAMTOOLS_STATS":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
