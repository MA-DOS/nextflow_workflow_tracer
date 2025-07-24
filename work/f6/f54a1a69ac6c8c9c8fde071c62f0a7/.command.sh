#!/bin/bash -euo pipefail
gtf2bed \
    genes.gtf \
    > genes.bed

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:GTF2BED":
    perl: $(echo $(perl --version 2>&1) | sed 's/.*v\(.*\)) built.*/\1/')
END_VERSIONS
