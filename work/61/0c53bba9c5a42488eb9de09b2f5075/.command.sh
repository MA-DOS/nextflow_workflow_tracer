#!/bin/bash -euo pipefail
INDEX=`find -L ./ -name "*.amb" | sed 's/\.amb$//'`

bwa mem \
    -M  -R '@RG\tID:OSMOTIC_STRESS_T150_SE_REP1_T2\tSM:OSMOTIC_STRESS_T150_SE_REP1\tPL:ILLUMINA\tLB:OSMOTIC_STRESS_T150_SE_REP1_T2\tPU:1' \
    -t 2 \
    $INDEX \
    OSMOTIC_STRESS_T150_SE_REP1_T2_trimmed.fq.gz \
    | samtools view -bhS -F 0x0100 -O BAM --threads 2 -o OSMOTIC_STRESS_T150_SE_REP1_T2.Lb.bam -

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_ALIGN_BWA:BWA_MEM":
    bwa: $(echo $(bwa 2>&1) | sed 's/^.*Version: //; s/Contact:.*$//')
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
