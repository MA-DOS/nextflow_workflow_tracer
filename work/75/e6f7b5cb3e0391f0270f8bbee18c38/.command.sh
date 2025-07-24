#!/bin/bash -euo pipefail
annotatePeaks.pl \
    consensus_peaks.mRp.clN.bed \
    genome.fa \
    -gid \
    -gtf genes.gtf \
    -cpu 2 \
    > consensus_peaks.mRp.clN.annotatePeaks.txt

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_CONSENSUS_PEAKS:HOMER_ANNOTATEPEAKS":
    homer: 4.11
END_VERSIONS
