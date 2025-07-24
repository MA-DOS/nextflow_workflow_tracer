#!/bin/bash -euo pipefail
bedGraphToBigWig \
    OSMOTIC_STRESS_T150_SE_REP1.mLb.clN.bedGraph \
    genome.fa.sizes \
    OSMOTIC_STRESS_T150_SE_REP1.mLb.clN.bigWig

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_BAM_TO_BIGWIG:UCSC_BEDGRAPHTOBIGWIG":
    ucsc: 445
END_VERSIONS
