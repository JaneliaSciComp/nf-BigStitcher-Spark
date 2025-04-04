process BIGSTITCHER_FUSE {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/bigstitcher:2.4.1-spark3.2.1-jdk8' }
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(fusion_container), val(spark)

    output:
    tuple val(meta), env(full_fusion_container), val(spark)

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def full_fusion_container_uri
    if (fusion_container.startsWith('s3://')) {
        // S3 bucket URI
        full_fusion_container_uri = fusion_container
    } else if (fusion_container.startsWith('gs://')) {
        // Google bucket URI
        full_fusion_container_uri = fusion_container
    } else if (fusion_container.startsWith('https://')) {
        // Http URI
        full_fusion_container_uri = fusion_container
    } else {
        full_fusion_container_uri = ''
    }
    """
    # if the fusion container is a Google or S3 bucket URI or an HTTP URI, use it as is
    if [[ "${full_fusion_container_uri}" == "" ]]; then
        full_fusion_container=\$(readlink -e ${fusion_container})
    else
        full_fusion_container=${fusion_container}
    fi

    /opt/scripts/runapp.sh \
        "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar \
        net.preibisch.bigstitcher.spark.SparkAffineFusion \
        -o \${full_fusion_container} \
        ${extra_args}

    """
}
