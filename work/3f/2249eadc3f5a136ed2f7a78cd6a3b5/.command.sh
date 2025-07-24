#!/bin/bash -euo pipefail
plotProfile \
     \
    --matrixFile OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.scale_regions.computeMatrix.mat.gz \
    --outFileName OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.reference_point.plotProfile.pdf \
    --outFileNameData OSMOTIC_STRESS_T15_PE_REP1.mLb.clN.reference_point.plotProfile.tab

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_BIGWIG_PLOT_DEEPTOOLS:DEEPTOOLS_PLOTPROFILE":
    deeptools: $(plotProfile --version | sed -e "s/plotProfile //g")
END_VERSIONS
