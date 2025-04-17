include { SPARK_START } from '../../janelia/spark_start/main'
include { SPARK_STOP  } from '../../janelia/spark_stop/main'

include { BIGSTITCHER_MODULE } from '../../../modules/local/bigstitcher/module/main'

workflow BIGSTITCHER_SPARK {
    take:
    ch_meta                 // channel: [ meta, [data_files_to_be_mounted] ]
    ch_module_input         // channel: [ meta, module_class, [module_args] ]
    spark_config            // map: Additional spark config properties
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
        spark_config,
        distributed_cluster,
        work_dir,
        spark_workers,
        min_spark_workers,
        spark_worker_cpus,
        spark_mem_gb_per_cpu,
        spark_driver_cpus,
        spark_driver_mem_gb,
    )
    | join(ch_module_input, by: 0)
    | join(ch_meta, by: 0)
    | map {
        def (meta, spark, module_class, module_args, data_files) = it
        
        def r = [
            [ meta, spark ],
            [ module_class, module_args ],
            data_files + [ spark.work_dir ]
        ]
        log.debug "BigStitcher inputs $it -> $r"
        r
    }

    BIGSTITCHER_MODULE(
        spark_input.map { it[0] }, // [ meta, spark ]
        spark_input.map { it[1] }, // [ module_class, module_args ]
        spark_input.map { it[2] }, // [ data_files ]
    )

    def bigstitcher_result = SPARK_STOP(
        BIGSTITCHER_MODULE.out, 
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
