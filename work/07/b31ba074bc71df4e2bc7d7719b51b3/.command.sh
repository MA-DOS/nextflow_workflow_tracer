#!/bin/bash -euo pipefail
picard \
    -Xmx4915M \
    MergeSamFiles \
    --SORT_ORDER coordinate --VALIDATION_STRINGENCY LENIENT --TMP_DIR tmp \
    --INPUT OSMOTIC_STRESS_T0_PE_REP2.mLb.clN.sorted.bam --INPUT OSMOTIC_STRESS_T0_PE_REP1.mLb.clN.sorted.bam \
    --OUTPUT OSMOTIC_STRESS_T0_PE.mRp.sorted.bam
cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PICARD_MERGESAMFILES_REPLICATE":
    picard: $( echo $(picard MergeSamFiles --version 2>&1) | grep -o 'Version:.*' | cut -f2- -d:)
END_VERSIONS
