#!/bin/bash -euo pipefail
samtools view \
    -F 0x004 -F 0x0008 -f 0x001 -F 0x0400 -q 1 \
    -L genome.include_regions.bed \
    -b OSMOTIC_STRESS_T0_PE_REP2.mLb.mkD.sorted.bam \
    | bamtools filter \
        -out OSMOTIC_STRESS_T0_PE_REP2.mLb.flT.sorted.bam \
        -script bamtools_filter_pe.json

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_FILTER_BAM:BAMTOOLS_FILTER":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
    bamtools: $(echo $(bamtools --version 2>&1) | sed 's/^.*bamtools //; s/Part .*$//')
END_VERSIONS
