#!/bin/bash -euo pipefail
cat genes.bed | awk -v FS='	' -v OFS='	' '{ if($6=="+") $3=$2+1; else $2=$3-1; print $1, $2, $3, $4, $5, $6;}' > genes.tss.bed

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:TSS_EXTRACT":
    sed: $(echo $(sed --version 2>&1) | sed 's/^.*GNU sed) //; s/ .*$//')
END_VERSIONS
