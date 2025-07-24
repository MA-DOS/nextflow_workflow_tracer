#!/bin/bash -euo pipefail
macs2 \
    callpeak \
    --keep-dup all --nomodel --broad --broad-cutoff 0.1 \
    --gsize 11624332 \
    --format BAM \
    --name OSMOTIC_STRESS_T100_SE_REP1.mLb.clN \
    --treatment OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam \


cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_CALL_ANNOTATE_PEAKS:MACS2_CALLPEAK":
    macs2: $(macs2 --version | sed -e "s/macs2 //g")
END_VERSIONS
