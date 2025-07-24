#!/bin/bash -euo pipefail
plot_homer_annotatepeaks.r \
    -i OSMOTIC_STRESS_T150_SE_REP1.mLb.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T100_SE_REP1.mLb.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T100_SE_REP2.mLb.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T0_PE_REP1.mLb.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T15_PE_REP1.mLb.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T0_PE_REP2.mLb.clN_peaks.annotatePeaks.txt \
    -s OSMOTIC_STRESS_T150_SE_REP1,OSMOTIC_STRESS_T100_SE_REP1,OSMOTIC_STRESS_T100_SE_REP2,OSMOTIC_STRESS_T0_PE_REP1,OSMOTIC_STRESS_T15_PE_REP1,OSMOTIC_STRESS_T0_PE_REP2 \
    -p macs2_annotatePeaks.mLb.clN \
    -o ./

find ./ -type f -name "*summary.txt" -exec cat {} \; | cat merged_library_peak_annotation_header.txt - > macs2_annotatePeaks.mLb.clN.summary_mqc.tsv

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_CALL_ANNOTATE_PEAKS:PLOT_HOMER_ANNOTATEPEAKS":
    r-base: $(echo $(R --version 2>&1) | sed 's/^.*R version //; s/ .*$//')
END_VERSIONS
