#!/bin/bash -euo pipefail
samtools view \
    -F 0x004 -F 0x0400 -q 1 \
    -L genome.include_regions.bed \
    -b OSMOTIC_STRESS_T100_SE_REP1.mLb.mkD.sorted.bam \
    | bamtools filter \
        -out OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam \
        -script bamtools_filter_se.json

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_FILTER_BAM:BAMTOOLS_FILTER":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
    bamtools: $(echo $(bamtools --version 2>&1) | sed 's/^.*bamtools //; s/Part .*$//')
END_VERSIONS
