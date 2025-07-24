#!/usr/bin/env bash -C -e -u -o pipefail
bgzip  --threads 1 -c  genome.bed > genome.bed.gz
tabix --threads 1  genome.bed.gz

cat <<-END_VERSIONS > versions.yml
"NFCORE_SAREK:PREPARE_INTERVALS:TABIX_BGZIPTABIX_INTERVAL_COMBINED":
    tabix: $(echo $(tabix -h 2>&1) | sed 's/^.*Version: //; s/ .*$//')
END_VERSIONS
