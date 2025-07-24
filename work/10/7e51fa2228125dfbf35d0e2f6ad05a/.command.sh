#!/bin/bash -euo pipefail
bedGraphToBigWig \
    OSMOTIC_STRESS_T100_SE.mRp.clN.bedGraph \
    genome.fa.sizes \
    OSMOTIC_STRESS_T100_SE.mRp.clN.bigWig

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_BAM_TO_BIGWIG:UCSC_BEDGRAPHTOBIGWIG":
    ucsc: 445
END_VERSIONS
