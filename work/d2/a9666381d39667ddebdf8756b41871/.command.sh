#!/bin/bash -euo pipefail
SCALE_FACTOR=$(grep '[0-9] mapped (' OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam.flagstat | awk '{print 1000000/$1}')
echo $SCALE_FACTOR > OSMOTIC_STRESS_T0_PE.mRp.clN.scale_factor.txt

bedtools \
    genomecov \
    -ibam OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam \
    -bg \
    -scale $SCALE_FACTOR \
    -pc \
     \
> tmp.bg

bedtools sort -i tmp.bg > OSMOTIC_STRESS_T0_PE.mRp.clN.bedGraph

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_BAM_TO_BIGWIG:BEDTOOLS_GENOMECOV":
    bedtools: $(bedtools --version | sed -e "s/bedtools v//g")
END_VERSIONS
