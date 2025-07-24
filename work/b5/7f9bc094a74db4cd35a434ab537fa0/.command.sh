#!/bin/bash -euo pipefail
computeMatrix \
    scale-regions --regionBodyLength 1000 --beforeRegionStartLength 3000 --afterRegionStartLength 3000 --missingDataAsZero --skipZeros --smartLabels \
    --regionsFileName genes.bed \
    --scoreFileName OSMOTIC_STRESS_T0_PE_REP2.mLb.clN.bigWig \
    --outFileName OSMOTIC_STRESS_T0_PE_REP2.mLb.clN.scale_regions.computeMatrix.mat.gz \
    --outFileNameMatrix OSMOTIC_STRESS_T0_PE_REP2.mLb.clN.scale_regions.computeMatrix.vals.mat.tab \
    --numberOfProcessors 2

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_BIGWIG_PLOT_DEEPTOOLS:DEEPTOOLS_COMPUTEMATRIX_SCALE_REGIONS":
    deeptools: $(computeMatrix --version | sed -e "s/computeMatrix //g")
END_VERSIONS
