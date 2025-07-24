#!/usr/bin/env bash -C -e -u -o pipefail
INDEX=`find -L ./ -name "*.amb" | sed 's/\.amb$//'`

bwa mem \
    -K 100000000 -Y -R "@RG\tID:test.test_L1\tPU:test_L1\tSM:test_test\tLB:test\tDS:https://raw.githubusercontent.com/nf-core/test-datasets/modules/data//genomics/homo_sapiens/genome/genome.fasta\tPL:ILLUMINA" \
    -t 4 \
    $INDEX \
    test_1.fastq.gz test_2.fastq.gz \
    | samtools sort   --threads 4 -o test-test_L1.sorted.bam -

cat <<-END_VERSIONS > versions.yml
"NFCORE_SAREK:SAREK:FASTQ_ALIGN_BWAMEM_MEM2_DRAGMAP_SENTIEON:BWAMEM1_MEM":
    bwa: $(echo $(bwa 2>&1) | sed 's/^.*Version: //; s/Contact:.*$//')
    samtools: $(echo $(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*$//')
END_VERSIONS
