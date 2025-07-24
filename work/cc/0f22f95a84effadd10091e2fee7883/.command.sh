#!/bin/bash -euo pipefail
get_autosomes.py \
    genome.fa.fai \
    genome.fa.autosomes.txt

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:GET_AUTOSOMES":
    python: $(python --version | sed 's/Python //g')
END_VERSIONS
