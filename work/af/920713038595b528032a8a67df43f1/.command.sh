#!/bin/bash -euo pipefail
SCALE_FACTOR=$(grep '[0-9] mapped (' OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam.flagstat | awk '{print 1000000/$1}')
echo $SCALE_FACTOR > OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.scale_factor.txt

bedtools \
    genomecov \
    -ibam OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.sorted.bam \
    -bg \
    -scale $SCALE_FACTOR \
     \
    -fs 200 \
> tmp.bg

bedtools sort -i tmp.bg > OSMOTIC_STRESS_T100_SE_REP1.mLb.clN.bedGraph

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_BAM_TO_BIGWIG:BEDTOOLS_GENOMECOV":
    bedtools: $(bedtools --version | sed -e "s/bedtools v//g")
END_VERSIONS
