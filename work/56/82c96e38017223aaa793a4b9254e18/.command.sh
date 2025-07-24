#!/bin/bash -euo pipefail
[ ! -f  OSMOTIC_STRESS_T100_SE_REP2_T1.fastq.gz ] && ln -s SRR1822154_1.fastq.gz OSMOTIC_STRESS_T100_SE_REP2_T1.fastq.gz
trim_galore \
    --fastqc \
    --cores 1 \
    --gzip \
    OSMOTIC_STRESS_T100_SE_REP2_T1.fastq.gz

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_FASTQC_UMITOOLS_TRIMGALORE:TRIMGALORE":
    trimgalore: $(echo $(trim_galore --version 2>&1) | sed 's/^.*version //; s/Last.*$//')
    cutadapt: $(cutadapt --version)
END_VERSIONS
