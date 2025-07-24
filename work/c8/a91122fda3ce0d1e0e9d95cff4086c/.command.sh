#!/bin/bash -euo pipefail
READS_IN_PEAKS=$(intersectBed -a OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam -b OSMOTIC_STRESS_T0_PE.mRp.clN_peaks.broadPeak -bed -c -f 0.20 | awk -F '	' '{sum += $NF} END {print sum}')
samtools flagstat OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam > OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam.flagstat
grep 'mapped (' OSMOTIC_STRESS_T0_PE.mRp.clN.sorted.bam.flagstat | grep -v "primary" | awk -v a="$READS_IN_PEAKS" -v OFS='	' '{print "OSMOTIC_STRESS_T0_PE", a/$1}' > OSMOTIC_STRESS_T0_PE.FRiP.txt

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_CALL_ANNOTATE_PEAKS:FRIP_SCORE":
    bedtools: $(bedtools --version | sed -e "s/bedtools v//g")
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
