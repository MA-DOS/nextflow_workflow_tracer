#!/bin/bash -euo pipefail
unique-kmers.py \
    -k 50 \
    -R report.txt \
     \
    genome.fa

grep ^number report.txt | sed 's/^.*:.[[:blank:]]//g' > kmers.txt

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:KHMER_UNIQUEKMERS":
    khmer: $( unique-kmers.py --version 2>&1 | grep ^khmer | sed 's/^khmer //;s/ .*$//' )
END_VERSIONS
