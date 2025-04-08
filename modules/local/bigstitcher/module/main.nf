process BIGSTITCHER_MODULE {
    tag { meta.id }
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/bigstitcher:2.4.1-spark3.2.1-jdk8' }
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta), path(bigstitcher_container), val(spark)
    val(module_class)
    val(module_args)

    output:
    tuple val(meta), env(full_bigstitcher_container), val(spark)

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = module_args ?: ''
    def executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    def driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')

    def full_bigstitcher_container_uri
    if (bigstitcher_container.startsWith('s3://')) {
        // S3 bucket URI
        full_bigstitcher_container_uri = bigstitcher_container
    } else if (bigstitcher_container.startsWith('gs://')) {
        // Google bucket URI
        full_bigstitcher_container_uri = bigstitcher_container
    } else if (bigstitcher_container.startsWith('https://')) {
        // Http URI
        full_bigstitcher_container_uri = bigstitcher_container
    } else {
        full_bigstitcher_container_uri = ''
    }
    """
    # if the fusion container is a Google bucket, S3 bucket, or an HTTP URI, use it as is
    if [[ "${full_bigstitcher_container_uri}" == "" ]]; then
        full_bigstitcher_container=\$(readlink -e ${bigstitcher_container})
    else
        full_bigstitcher_container=${bigstitcher_container}
    fi

    echo 'Bigstitcher cmd:' /opt/scripts/runapp.sh \
        "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar \
        ${module_class} \
        ${spark.parallelism} \
        ${spark.worker_cores} \
        ${executor_memory} \
        ${spark.driver_cores} \
        ${driver_memory} \
        -o \${full_bigstitcher_container} \
        ${extra_args}


    HOME=/tmp/home
    mkdir -p \${HOME}

    /opt/scripts/runapp.sh \
        "${workflow.containerEngine}" "${spark.work_dir}" "${spark.uri}" \
        /app/app.jar \
        ${module_class} \
        ${spark.parallelism} \
        ${spark.worker_cores} \
        ${executor_memory} \
        ${spark.driver_cores} \
        ${driver_memory} \
        -o \${full_bigstitcher_container} \
        ${extra_args}

    """
}
