#!/bin/bash -euo pipefail
printf "%s %s\n" SRR1822153_1.fastq.gz OSMOTIC_STRESS_T0_PE_REP1_T1_1.gz SRR1822153_2.fastq.gz OSMOTIC_STRESS_T0_PE_REP1_T1_2.gz | while read old_name new_name; do
    [ -f "${new_name}" ] || ln -s $old_name $new_name
done

fastqc \
    --quiet \
    --threads 2 \
    OSMOTIC_STRESS_T0_PE_REP1_T1_1.gz OSMOTIC_STRESS_T0_PE_REP1_T1_2.gz

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:FASTQ_FASTQC_UMITOOLS_TRIMGALORE:FASTQC":
    fastqc: $( fastqc --version | sed -e "s/FastQC v//g" )
END_VERSIONS
