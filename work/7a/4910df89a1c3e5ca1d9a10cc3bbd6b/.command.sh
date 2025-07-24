#!/bin/bash -euo pipefail
awk '{print $1, '0' , $2}' OFS='	' genome.fa.sizes | awk '$1 !~ /MT/ {print $0}' > genome.include_regions.bed

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:PREPARE_GENOME:GENOME_BLACKLIST_REGIONS":
    bedtools: $(bedtools --version | sed -e "s/bedtools v//g")
END_VERSIONS
