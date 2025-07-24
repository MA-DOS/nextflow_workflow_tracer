#!/bin/bash -euo pipefail
plot_macs2_qc.r \
    -i OSMOTIC_STRESS_T100_SE_REP1.mLb.clN_peaks.broadPeak,OSMOTIC_STRESS_T100_SE_REP2.mLb.clN_peaks.broadPeak,OSMOTIC_STRESS_T150_SE_REP1.mLb.clN_peaks.broadPeak,OSMOTIC_STRESS_T0_PE_REP2.mLb.clN_peaks.broadPeak,OSMOTIC_STRESS_T0_PE_REP1.mLb.clN_peaks.broadPeak,OSMOTIC_STRESS_T15_PE_REP1.mLb.clN_peaks.broadPeak \
    -s OSMOTIC_STRESS_T100_SE_REP1.mLb.clN,OSMOTIC_STRESS_T100_SE_REP2.mLb.clN,OSMOTIC_STRESS_T150_SE_REP1.mLb.clN,OSMOTIC_STRESS_T0_PE_REP2.mLb.clN,OSMOTIC_STRESS_T0_PE_REP1.mLb.clN,OSMOTIC_STRESS_T15_PE_REP1.mLb.clN \
    -o ./ -p macs2_peak.mLb.clN

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_CALL_ANNOTATE_PEAKS:PLOT_MACS2_QC":
    r-base: $(echo $(R --version 2>&1) | sed 's/^.*R version //; s/ .*$//')
END_VERSIONS
