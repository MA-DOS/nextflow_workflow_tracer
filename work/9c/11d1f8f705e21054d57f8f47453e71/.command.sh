#!/bin/bash -euo pipefail
samtools faidx genome.fa
cut -f 1,2 genome.fa.fai > genome.fa.sizes

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:CUSTOM_GETCHROMSIZES":
    getchromsizes: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
