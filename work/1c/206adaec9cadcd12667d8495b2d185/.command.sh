#!/bin/bash -euo pipefail
annotatePeaks.pl \
    consensus_peaks.mLb.clN.bed \
    genome.fa \
    -gid \
    -gtf genes.gtf \
    -cpu 2 \
    > consensus_peaks.mLb.clN.annotatePeaks.txt

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_CONSENSUS_PEAKS:HOMER_ANNOTATEPEAKS":
    homer: 4.11
END_VERSIONS
