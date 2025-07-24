#!/bin/bash -euo pipefail
picard \
    -Xmx4915M \
    CollectMultipleMetrics \
    --VALIDATION_STRINGENCY LENIENT --TMP_DIR tmp \
    --INPUT OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.sorted.bam \
    --OUTPUT OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.CollectMultipleMetrics \
    --REFERENCE_SEQUENCE genome.fa

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_PICARD_COLLECTMULTIPLEMETRICS":
    picard: $(picard CollectMultipleMetrics --version 2>&1 | grep -o 'Version.*' | cut -f2- -d:)
END_VERSIONS
