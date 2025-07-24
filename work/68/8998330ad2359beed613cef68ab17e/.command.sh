#!/bin/bash -euo pipefail
ln -s OSMOTIC_STRESS_T100_SE_REP2_T1.Lb.sorted.bam OSMOTIC_STRESS_T100_SE_REP2.mLb.sorted.bam
cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PICARD_MERGESAMFILES_LIBRARY":
    picard: $( echo $(picard MergeSamFiles --version 2>&1) | grep -o 'Version:.*' | cut -f2- -d:)
END_VERSIONS
