#!/bin/bash -euo pipefail
ataqv \
    --ignore-read-groups \
    --mitochondrial-reference-name MT \
    --peak-file OSMOTIC_STRESS_T0_PE_REP2.mLb.clN_peaks.broadPeak \
    --tss-file genes.tss.bed \
     \
    --autosomal-reference-file genome.fa.autosomes.txt \
    --metrics-file "OSMOTIC_STRESS_T0_PE_REP2.ataqv.json" \
    --threads 2 \
    --name OSMOTIC_STRESS_T0_PE_REP2 \
    NA \
    OSMOTIC_STRESS_T0_PE_REP2.mLb.mkD.sorted.bam

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_ATAQV_ATAQV":
    ataqv: $( ataqv --version )
END_VERSIONS
