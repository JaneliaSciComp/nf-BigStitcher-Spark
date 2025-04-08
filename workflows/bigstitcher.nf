include { BIGSTITCHER_SPARK } from '../subworkflows//local/bigstitcher_spark'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    INPUT AND VARIABLES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

module_class = get_module_class(params.module)
module_params = params.module_params

if (params.xml) {
    xml_file = file("${params.xml}")
} 
else { 
    xml_file = null 
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow BIGSTITCHER {

    main:

    ch_versions = Channel.empty()


    //
    // Create channel from input file and output dir provided through params.input and params.outdir
    //

    Channel.of(file(params.outdir))
        .map { output_dir ->
            def meta = [ id: "bigstitcher" ]
            data_files = []
            if (xml_file) {
                data_files << file(xml_file)
            }
            if (output_dir) {
                data_files << file(output_dir)
            }
            def input_data = [
                meta,
                data_files,
            ]
            log.debug "Input data: ${module_class} ${input_data} ${module_params}"
            // return input_data
            input_data
        }
        .set { ch_data }

    BIGSTITCHER_SPARK(
        ch_data,
        module_class,
        module_params,
        params.bigstitcher_distributed_cluster,
        params.work_dir,
        params.bigstitcher_spark_workers,
        params.bigstitcher_min_spark_workers,
        params.bigstitcher_spark_worker_cpus,
        params.bigstitcher_spark_mem_gb_per_cpu,
        params.bigstitcher_spark_driver_cpus,
        params.bigstitcher_spark_driver_mem_gb
    )

    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(
            storeDir: "${params.outdir}/pipeline_info",
            name: 'nf_core_'  +  'bigstitcher_software_'  + 'versions.yml',
            sort: true,
            newLine: true
        ).set { ch_collated_versions }

    emit:
    versions = ch_collated_versions  // channel: [ path(versions.yml) ]
}

//
// Get the module Java class for the given module name
//
def get_module_class(module) {
    switch(module) {
        case 'resave':
            return 'net.preibisch.bigstitcher.spark.SparkResaveN5'
        case 'stitching':
            return 'net.preibisch.bigstitcher.spark.SparkPairwiseStitching'
        case 'affine-fusion':
            return 'net.preibisch.bigstitcher.spark.SparkAffineFusion'
        default:
            error "Unsupported module: ${module}"
    }
}