#!/bin/bash -euo pipefail
computeMatrix \
    reference-point --missingDataAsZero --skipZeros --smartLabels --upstream 3000 --downstream 3000 \
    --regionsFileName genes.tss.bed \
    --scoreFileName OSMOTIC_STRESS_T0_PE_REP1.mLb.clN.bigWig \
    --outFileName OSMOTIC_STRESS_T0_PE_REP1.mLb.clN.reference_point.computeMatrix.mat.gz \
    --outFileNameMatrix OSMOTIC_STRESS_T0_PE_REP1.mLb.clN.reference_point.computeMatrix.vals.mat.tab \
    --numberOfProcessors 2

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_BIGWIG_PLOT_DEEPTOOLS:DEEPTOOLS_COMPUTEMATRIX_REFERENCE_POINT":
    deeptools: $(computeMatrix --version | sed -e "s/computeMatrix //g")
END_VERSIONS
