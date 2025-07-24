#!/bin/bash -euo pipefail
[ ! -f  OSMOTIC_STRESS_T0_PE_REP2_T1_1.fastq.gz ] && ln -s SRR1822154_1.fastq.gz OSMOTIC_STRESS_T0_PE_REP2_T1_1.fastq.gz
[ ! -f  OSMOTIC_STRESS_T0_PE_REP2_T1_2.fastq.gz ] && ln -s SRR1822154_2.fastq.gz OSMOTIC_STRESS_T0_PE_REP2_T1_2.fastq.gz
trim_galore \
    --fastqc \
    --cores 1 \
    --paired \
    --gzip \
    OSMOTIC_STRESS_T0_PE_REP2_T1_1.fastq.gz \
    OSMOTIC_STRESS_T0_PE_REP2_T1_2.fastq.gz

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_FASTQC_UMITOOLS_TRIMGALORE:TRIMGALORE":
    trimgalore: $(echo $(trim_galore --version 2>&1) | sed 's/^.*version //; s/Last.*$//')
    cutadapt: $(cutadapt --version)
END_VERSIONS
