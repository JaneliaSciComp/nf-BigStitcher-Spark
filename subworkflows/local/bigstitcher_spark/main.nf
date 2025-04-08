include { SPARK_START } from '../../janelia/spark_start/main'
include { SPARK_STOP  } from '../../janelia/spark_stop/main'

include { BIGSTITCHER_MODULE } from '../../../modules/local/bigstitcher/module/main'

workflow BIGSTITCHER_SPARK {
    take:
    ch_meta                 // channel: [ meta, [dataset.xml, fusion_container] ]
    bigstitcher_class       // string: Java class for the BigStitcher module
    bigstitcher_args        // string: arguments for the BigStitcher module
    distributed_cluster     // boolean: use a distributed cluster
    work_dir                // string | file: working directory
    spark_workers           // int: number of workers in the cluster
    min_spark_workers       // int: minimum required workers
    spark_worker_cpus       // int: number of CPUs per worker
    spark_mem_gb_per_cpu    // int: memory in GB per worker core
    spark_driver_cpus       // int: number of CPUs for the driver  
    spark_driver_mem_gb     // int: driver memory in GB

    main:

    def spark_input = SPARK_START(
        ch_meta,
        distributed_cluster,
        work_dir,
        spark_workers,
        min_spark_workers,
        spark_worker_cpus,
        spark_mem_gb_per_cpu,
        spark_driver_cpus,
        spark_driver_mem_gb,
    )
    | join(ch_meta, by: 0)
    | map {
        def (meta, spark, fusion_container) = it
        [ meta, fusion_container, spark ]
    }

    BIGSTITCHER_MODULE(
        spark_input,
        bigstitcher_class,
        bigstitcher_args,
    )

    def bigstitcher_result = SPARK_STOP(
        BIGSTITCHER_MODULE.out.map { [ /*meta*/it[0], /*spark*/it[2]] }, 
        distributed_cluster,
    ) | map {
        def (meta, spark) = it
        [ meta, spark ]
        log.debug "Stopped BigStitcher.Fuse spark cluster: ${spark} -> ${meta}"
        meta
    }

    emit:
    done = bigstitcher_result

}
