#!/bin/bash -euo pipefail
plot_homer_annotatepeaks.r \
    -i OSMOTIC_STRESS_T0_PE.mRp.clN_peaks.annotatePeaks.txt,OSMOTIC_STRESS_T100_SE.mRp.clN_peaks.annotatePeaks.txt \
    -s OSMOTIC_STRESS_T0_PE,OSMOTIC_STRESS_T100_SE \
    -p macs2_annotatePeaks.mRp.clN \
    -o ./

find ./ -type f -name "*summary.txt" -exec cat {} \; | cat merged_replicate_peak_annotation_header.txt - > macs2_annotatePeaks.mRp.clN.summary_mqc.tsv

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_CALL_ANNOTATE_PEAKS:PLOT_HOMER_ANNOTATEPEAKS":
    r-base: $(echo $(R --version 2>&1) | sed 's/^.*R version //; s/ .*$//')
END_VERSIONS
