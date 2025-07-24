#!/bin/bash -euo pipefail
picard \
    -Xmx4915M \
    MergeSamFiles \
    --SORT_ORDER coordinate --VALIDATION_STRINGENCY LENIENT --TMP_DIR tmp \
    --INPUT OSMOTIC_STRESS_T150_SE_REP1_T1.Lb.sorted.bam --INPUT OSMOTIC_STRESS_T150_SE_REP1_T2.Lb.sorted.bam \
    --OUTPUT OSMOTIC_STRESS_T150_SE_REP1.mLb.sorted.bam
cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PICARD_MERGESAMFILES_LIBRARY":
    picard: $( echo $(picard MergeSamFiles --version 2>&1) | grep -o 'Version:.*' | cut -f2- -d:)
END_VERSIONS
