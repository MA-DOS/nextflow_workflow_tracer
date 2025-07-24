#!/bin/bash -euo pipefail
deseq2_qc.r \
    --count_file consensus_peaks.mRp.clN.featureCounts.txt \
    --outdir ./ \
    --outprefix consensus_peaks.mRp.clN \
    --cores 2 \
    --id_col 1 --sample_suffix '.mLb.clN.sorted.bam' --count_col 7 --vst TRUE

sed 's/deseq2_pca/deseq2_pca_1/g' <merged_replicate_deseq2_pca_header.txt >tmp.txt
sed -i -e 's/DESeq2 /consensus_peaks DESeq2 /g' tmp.txt
cat tmp.txt consensus_peaks.mRp.clN.pca.vals.txt > consensus_peaks.mRp.clN.pca.vals_mqc.tsv

sed 's/deseq2_clustering/deseq2_clustering_1/g' <merged_replicate_deseq2_clustering_header.txt >tmp.txt
sed -i -e 's/DESeq2 /consensus_peaks DESeq2 /g' tmp.txt
cat tmp.txt consensus_peaks.mRp.clN.sample.dists.txt > consensus_peaks.mRp.clN.sample.dists_mqc.tsv

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_REPLICATE_CONSENSUS_PEAKS:DESEQ2_QC":
    r-base: $(echo $(R --version 2>&1) | sed 's/^.*R version //; s/ .*$//')
    bioconductor-deseq2: $(Rscript -e "library(DESeq2); cat(as.character(packageVersion('DESeq2')))")
END_VERSIONS
