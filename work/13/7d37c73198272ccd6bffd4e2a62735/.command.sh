#!/bin/bash -euo pipefail
cat OSMOTIC_STRESS_T100_SE.mRp.clN_peaks.broadPeak | wc -l | awk -v OFS='	' '{ print "OSMOTIC_STRESS_T100_SE.mRp.clN_peaks", $1 }' | cat merged_replicate_peak_count_header.txt - > OSMOTIC_STRESS_T100_SE.mRp.clN_peaks.count_mqc.tsv
cat merged_replicate_frip_score_header.txt OSMOTIC_STRESS_T100_SE.FRiP.txt > OSMOTIC_STRESS_T100_SE.mRp.clN_peaks.FRiP_mqc.tsv

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_CALL_ANNOTATE_PEAKS:MULTIQC_CUSTOM_PEAKS":
    sed: $(echo $(sed --version 2>&1) | sed 's/^.*GNU sed) //; s/ .*$//')
END_VERSIONS
