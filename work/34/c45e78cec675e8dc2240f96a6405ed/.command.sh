#!/bin/bash -euo pipefail
samtools sort \
     \
    -@ 2 \
    -o OSMOTIC_STRESS_T0_PE_REP1_T1.Lb.sorted.bam \
    -T OSMOTIC_STRESS_T0_PE_REP1_T1.Lb.sorted \
    OSMOTIC_STRESS_T0_PE_REP1_T1.Lb.bam

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_ALIGN_BWA:BAM_SORT_STATS_SAMTOOLS:SAMTOOLS_SORT":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
