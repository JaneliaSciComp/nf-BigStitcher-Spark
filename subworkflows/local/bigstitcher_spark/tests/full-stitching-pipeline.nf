include { BIGSTITCHER_SPARK as STITCH           } from '../main'
include { BIGSTITCHER_SPARK as CREATE_CONTAINER } from '../main'
include { BIGSTITCHER_SPARK as FUSE             } from '../main'

workflow FULL_STITCHING_WORKFLOW {
    take:
    ch_input                // channel: [ val(meta), path(stitching_data) ]
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
    def p1 = spark_config
    def p2 = distributed_cluster
    def p3 = work_dir
    def p4 = spark_workers
    def p5 = min_spark_workers
    def p6 = spark_worker_cpus
    def p7 = spark_mem_gb_per_cpu
    def p8 = spark_driver_cpus
    def p9 = spark_driver_mem_gb

    def stitching_inputs = ch_input
    | multiMap {
        def (meta, stitching_data) = it
        log.debug "Stitching data: ${meta}, ${stitching_data}"
        ch_meta: [ meta, [stitching_data] ]
        ch_module_input: [
            meta,
            'net.preibisch.bigstitcher.spark.SparkPairwiseStitching',
            [
                '-x', "${stitching_data}/dataset.xml",
            ],
        ]
    }

    STITCH(
        stitching_inputs.ch_meta,
        stitching_inputs.ch_module_input,
        p1,
        p2,
        "${p3}/stitch",
        p4,
        p5,
        p6,
        p7,
        p8,
        p9,
    )

    def stitching_results = ch_input
    | join(STITCH.out, by:0)
    | map {
        def (meta, stitching_data) = it
        [ meta, stitching_data, "${stitching_data}/dataset.xml" ]
    }

    def create_container_inputs = stitching_results
    | multiMap {
        def (meta, stitching_data, dataset_proj_xml) = it
        ch_meta: [ meta, [stitching_data] ]
        ch_module_input: [
            meta,
            'net.preibisch.bigstitcher.spark.CreateFusionContainer',
            [
                '-x', dataset_proj_xml,
                '-o', "${stitching_data}/fused.zarr",
                '--preserveAnisotropy --multiRes',
            ],
        ]
    }

    CREATE_CONTAINER(
        create_container_inputs.ch_meta,
        create_container_inputs.ch_module_input,
        p1,
        false,
        "${p3}/create-container",
        p4,
        p5,
        p6,
        p7,
        p8,
        p9,
    )

    def fuse_inputs = ch_input
    | join(CREATE_CONTAINER.out, by:0)
    | multiMap {
        def (meta, stitching_data) = it
        ch_meta: [ meta, [stitching_data] ]
        ch_module_input: [
            meta,
            'net.preibisch.bigstitcher.spark.SparkAffineFusion',
            [
                '-o', "${stitching_data}/fused.zarr",
            ],
        ]
    }

    FUSE(
        fuse_inputs.ch_meta,
        fuse_inputs.ch_module_input,
        p1,
        p2,
        "${p3}/fuse",
        p4,
        p5,
        p6,
        p7,
        p8,
        p9,
    )

    def fuse_results = ch_input
    | join(FUSE.out, by:0)
    | map {
        def (meta, stitching_data) = it
        [ meta, stitching_data, "${stitching_data}/fused.zarr" ]
    }

    emit:
    s1_res = stitching_results
    s2_res = fuse_results
}
