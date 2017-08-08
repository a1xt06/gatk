workflow CNVOncotateCalledSegments {
    File called_file
    Int? mem
    String? oncotator_docker
    String? oncotator_output_type
    String? additional_args

    call OncotateCnvSegments {
        input:
            called_file=called_file,
            mem=mem,
            oncotator_docker=oncotator_docker,
            oncotator_output_type=oncotator_output_type,
            additional_args=additional_args
    }

    output {
        File oncotated_called_file = OncotateCnvSegments.oncotated_called_file
    }
}

task OncotateCnvSegments {
    File called_file
    Int mem=2
    String oncotator_docker="broadinstitute/oncotator:1.9.3.0-eval-gatk-protected"
    String oncotator_output_type="SIMPLE_TSV"
    String additional_args=""
    String basename_called_file = basename(called_file)
    command {
        set -e
        /root/oncotator_venv/bin/oncotator --db-dir /root/onco_dbdir/ -c /root/tx_exact_uniprot_matches.AKT1_CRLF2_FGFR1.txt \
          -u file:///root/onco_cache/ -r -v ${called_file} ${basename_called_file}.per_segment.oncotated.txt hg19 \
          -i SEG_FILE -o ${oncotator_output_type} ${additional_args}
    }

    output {
        File oncotated_called_file = "${basename_called_file}.per_segment.oncotated.txt"
    }

    runtime {
        docker: "${oncotator_docker}"
        memory: "${mem} GB"
    }
}