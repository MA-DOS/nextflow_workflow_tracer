#!/bin/bash -euo pipefail
ataqv \
    --ignore-read-groups \
    --mitochondrial-reference-name MT \
    --peak-file OSMOTIC_STRESS_T100_SE_REP1.mLb.clN_peaks.broadPeak \
    --tss-file genes.tss.bed \
     \
    --autosomal-reference-file genome.fa.autosomes.txt \
    --metrics-file "OSMOTIC_STRESS_T100_SE_REP1.ataqv.json" \
    --threads 2 \
    --name OSMOTIC_STRESS_T100_SE_REP1 \
    NA \
    OSMOTIC_STRESS_T100_SE_REP1.mLb.mkD.sorted.bam

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_ATAQV_ATAQV":
    ataqv: $( ataqv --version )
END_VERSIONS
