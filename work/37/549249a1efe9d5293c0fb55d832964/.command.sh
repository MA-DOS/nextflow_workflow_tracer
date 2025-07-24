#!/bin/bash -euo pipefail
mkarv \
     \
    --concurrency 2 \
    --force \
    ./html/ \
    jsons/*

cat <<-END_VERSIONS > versions.yml
"NFCORE_ATACSEQ:ATACSEQ:MERGED_LIBRARY_ATAQV_MKARV":
    # mkarv: $( mkarv --version ) # Use this when version string has been fixed
    ataqv: $( ataqv --version )
END_VERSIONS
