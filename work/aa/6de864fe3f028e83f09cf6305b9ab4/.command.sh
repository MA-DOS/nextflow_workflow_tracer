#!/bin/bash -euo pipefail
find * -type l -name "*.bigWig" -exec echo -e ""{}"\t0,0,178" \; | { grep "^bwa/merged_library/bigwig" || test $? = 1; } > mLb_bigwig.igv.txt
find * -type l -name "*Peak" -exec echo -e ""{}"\t0,0,178" \; | { grep "^bwa/merged_library/macs2/broad_peak" || test $? = 1; } > mLb_peaks.igv.txt
find * -type l -name "*.bed" -exec echo -e ""{}"\t0,0,0" \; | { grep "^bwa/merged_library/macs2/broad_peak/consensus" || test $? = 1; } > mLb_bed.igv.txt
find * -type l -name "*.bigWig" -exec echo -e ""{}"\t0,0,178" \; | { grep "^bwa/merged_replicate/bigwig" || test $? = 1; } > mRp_bigwig.igv.txt
find * -type l -name "*Peak" -exec echo -e ""{}"\t0,0,178" \; | { grep "^bwa/merged_replicate/macs2/broad_peak" || test $? = 1; } > mRp_peaks.igv.txt
find * -type l -name "*.bed" -exec echo -e ""{}"\t0,0,0" \; | { grep "^bwa/merged_replicate/macs2/broad_peak/consensus" || test $? = 1; } > mRp_bed.igv.txt

cat *.txt > igv_files.txt
igv_files_to_session.py igv_session.xml igv_files.txt ../../genome/genome.fa --path_prefix '../../'

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:IGV":
    python: $(python --version | sed 's/Python //g')
END_VERSIONS
