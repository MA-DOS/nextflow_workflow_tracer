#!/bin/bash -euo pipefail
check_samplesheet.py \
    samplesheet_test.csv \
    samplesheet.valid.csv

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:INPUT_CHECK:SAMPLESHEET_CHECK":
    python: $(python --version | sed 's/Python //g')
END_VERSIONS
