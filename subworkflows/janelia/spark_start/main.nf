process SPARK_STARTMANAGER {
    label 'process_long'
    container 'ghcr.io/janeliascicomp/spark:3.3.2-scala2.12-java17-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir)

    output:
    tuple val(meta), val(spark), env(full_spark_work_dir)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    spark_local_dir = task.ext.spark_local_dir ?: "/tmp/spark-${workflow.sessionId}"
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_config_filepath = "\${full_spark_work_dir}/spark-defaults.conf"
    spark_master_log_file = "\${full_spark_work_dir}/sparkmaster.log"
    terminate_file_name = "\${full_spark_work_dir}/terminate-spark"
    container_engine = workflow.containerEngine
    """
    full_spark_work_dir=\$(readlink -m ${spark_work_dir})
    if [[ ! -e \${full_spark_work_dir} ]] ; then
        echo "Create spark work directory ${spark_work_dir} -> \${full_spark_work_dir}"
        mkdir -p \${full_spark_work_dir}
    else
        echo "Spark work directory: \${full_spark_work_dir} - already exists"
    fi

    CMD=(
        /opt/scripts/startmanager.sh
        "$spark_local_dir"
        \${full_spark_work_dir}
        "$spark_master_log_file"
        "$spark_config_filepath"
        "$terminate_file_name"
        "$args"
        $sleep_secs $container_engine
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}

process SPARK_WAITFORMANAGER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.3.2-scala2.12-java17-ubuntu24.04'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate'
    }
    maxRetries 20

    input:
    tuple val(meta), val(spark), path(spark_work_dir)

    output:
    tuple val(meta), val(spark), env(full_spark_work_dir), env(spark_uri)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_master_log_name = "\${full_spark_work_dir}/sparkmaster.log"
    terminate_file_name = "\${full_spark_work_dir}/terminate-spark"
    """
    full_spark_work_dir=\$(readlink -m ${spark_work_dir})

    CMD=(
        /opt/scripts/waitformanager.sh
        "$spark_master_log_name"
        "$terminate_file_name"
        $sleep_secs
        $max_wait_secs
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    spark_uri=\$(cat spark_uri)

    echo "Export spark URI: \${spark_uri}"
    """
}

process SPARK_STARTWORKER {
    label 'process_long'
    container 'ghcr.io/janeliascicomp/spark:3.3.2-scala2.12-java17-ubuntu24.04'
    cpus { spark.worker_cores }
    memory { spark.worker_memory }

    input:
    tuple val(meta), val(spark), path(spark_work_dir), val(worker_id)
    path(data_paths, stageAs: '?/*')

    output:
    tuple val(meta), val(spark), env(full_spark_work_dir), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_worker_log_file = "\${full_spark_work_dir}/sparkworker-${worker_id}.log"
    spark_config_filepath = "\${full_spark_work_dir}/spark-defaults.conf"
    terminate_file_name = "\${full_spark_work_dir}/terminate-spark"
    worker_memory = spark.worker_memory.replace(" KB",'').replace(" MB",'').replace(" GB",'').replace(" TB",'')
    container_engine = workflow.containerEngine
    """
    full_spark_work_dir=\$(readlink -m ${spark_work_dir})

    CMD=(
        /opt/scripts/startworker.sh
        "\${full_spark_work_dir}"
        "${spark.uri}"
        "$worker_id"
        "${spark.worker_cores}"
        "${worker_memory}"
        "$spark_worker_log_file"
        "$spark_config_filepath"
        "$terminate_file_name"
        "$args"
        "$sleep_secs"
        "$container_engine"
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}

process SPARK_WAITFORWORKER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.3.2-scala2.12-java17-ubuntu24.04'
    // retry on a timeout to prevent the case when the waiter is started
    // before the worker and the worker never gets its chance
    errorStrategy { task.exitStatus == 2 ? 'retry' : 'terminate' }
    maxRetries 20

    input:
    tuple val(meta), val(spark), path(spark_work_dir), val(worker_id)

    output:
    tuple val(meta), val(spark), env(full_spark_work_dir), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_worker_log_file = "\${full_spark_work_dir}/sparkworker-${worker_id}.log"
    terminate_file_name = "\${full_spark_work_dir}/terminate-spark"
    """
    full_spark_work_dir=\$(readlink -m ${spark_work_dir})

    CMD=(
        /opt/scripts/waitforworker.sh
        "${spark.uri}"
        "$spark_worker_log_file"
        "$terminate_file_name"
        $sleep_secs
        $max_wait_secs
    )
    (exec "\${CMD[@]}")
    """
}

process SPARK_CLEANUP {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.3.2-scala2.12-java17-ubuntu24.04'

    input:
    tuple val(meta), val(spark), path(spark_work_dir)

    output:
    tuple val(meta), val(spark)

    script:
    """
    full_spark_work_dir=\$(readlink -m ${spark_work_dir})

    find \${full_spark_work_dir} -name app.jar -exec rm {} \\;
    """
}

/**
 * Prepares a Spark context using either a distributed cluster or single process
 * as specified by the `spark_cluster` parameter.
 */
workflow SPARK_START {
    take:
    ch_meta               // channel: [ meta, [data_paths] ]
    spark_cluster         // boolean: use a distributed cluster?
    working_dir           // path: shared storage path for worker communication
    spark_workers         // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_workers           // int: number of minimum required workers
    spark_worker_cpus     // int: number of cores per worker
    spark_gb_per_cpu      // int: number of GB of memory per worker core
    spark_driver_cpus     // int: number of cores for the driver
    spark_driver_mem_gb   // int: number of GB of memory for the driver

    main:
    // create a Spark context for each meta
    def n_spark_workers = spark_workers ?: 1
    def meta_and_spark = ch_meta
    | map {
        def (meta, data_paths) = it // ch_meta
        def spark_work_dir = file("${working_dir}/spark/${meta.id}")
        def spark = [:]
        spark.work_dir = spark_work_dir
        spark.workers = n_spark_workers
        spark.worker_cores = spark_worker_cpus ?: 1
        spark.driver_cores = spark_driver_cpus ?: 1
        def spark_driver_mem_gb_value = spark_driver_mem_gb ?: (spark.driver_cores * spark_gb_per_cpu)
        spark.driver_memory = "${spark_driver_mem_gb_value} GB"
        spark.parallelism = (n_spark_workers * spark_worker_cpus)
        // 1 GB of overhead for Spark, the rest for executors
        spark.worker_memory = (spark_worker_cpus * spark_gb_per_cpu + 1)+" GB"
        spark.executor_memory = (spark_worker_cpus * spark_gb_per_cpu)+" GB"
        def r = [
            meta,
            spark,
            spark_work_dir,
        ]
        log.debug "Prepare to start spark: $r"
        r
    }

    if (spark_cluster) {
        // start the Spark manager
        // this runs indefinitely until SPARK_TERMINATE is called
        SPARK_STARTMANAGER(meta_and_spark)

        // start a watcher that waits for the manager to be ready
        SPARK_WAITFORMANAGER(meta_and_spark)

        // prepare all arguments for all workers
        def meta_workers_with_data = SPARK_WAITFORMANAGER.out
        | join(ch_meta, by:0) // join with ch_meta to get the data files in order to mount them in the workers
        | flatMap {
            def (meta, spark, spark_work_dir, spark_uri, data_paths) = it
            log.debug "Spark manager available: ${meta}: ${spark} using spark work dir: ${spark_work_dir}"
            spark.uri = spark_uri
            def worker_list = 1..spark.workers
            worker_list.collect { worker_id ->
                [ meta, spark, spark_work_dir, worker_id, data_paths ]
            }
        }
        | multiMap {
            def (meta, spark, spark_work_dir, worker_id, data_paths) = it
            log.debug "Spark worker input: $it"
            worker: [ meta, spark, spark_work_dir, worker_id ]
            data: data_paths
        }

        // start workers
        // these run indefinitely until SPARK_TERMINATE is called
        SPARK_STARTWORKER(
            meta_workers_with_data.worker,
            meta_workers_with_data.data,
        )
        SPARK_STARTWORKER.out.groupTuple(by:[0,1,2], size: n_spark_workers)
        | map {
            def (meta, spark, spark_work_dir, worker_ids) = it
            // worker_ids are not needed for cleanup process
            [ meta, spark, spark_work_dir ]
        }
        | SPARK_CLEANUP // when workers exit they should clean up after themselves

        // wait for all workers to start
        def needed_workers = min_workers <= 0 || min_workers > n_spark_workers
                                ? n_spark_workers
                                : min_workers
        spark_context = SPARK_WAITFORWORKER(meta_workers_with_data.worker)
        | groupTuple(by: [0,1], size: needed_workers)
        | map {
            def (meta, spark, spark_work_dir, worker_ids) = it
            log.debug "Create distributed Spark context: ${meta}, ${spark}"
            [ meta, spark ]
        }
    } else {
        // when running locally, the driver needs enough resources to run a spark worker
        spark_context = meta_and_spark.map {
            def (meta, spark, spark_work_dir) = it
            spark.workers = 1
            spark.driver_cores = spark_driver_cpus + spark_worker_cpus
            spark.driver_memory = (2 + spark_worker_cpus * spark_gb_per_cpu) + " GB"
            spark.uri = 'local[*]'
            log.debug "Create local Spark context: ${meta}, ${spark}"
            [ meta, spark ]
        }
    }

    emit:
    spark_context // channel: [ val(meta), val(spark) ]
}
