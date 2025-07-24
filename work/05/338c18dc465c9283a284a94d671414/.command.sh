#!/bin/bash -euo pipefail
bampe_rm_orphan.py \
    OSMOTIC_STRESS_T0_PE_REP1.mLb.flT.name_sorted.bam \
    OSMOTIC_STRESS_T0_PE_REP1.mLb.clN.bam \
    --only_fr_pairs

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_FILTER_BAM:BAM_REMOVE_ORPHANS":
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
