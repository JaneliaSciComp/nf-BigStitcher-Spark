include { BIGSTITCHER_SPARK } from '../subworkflows//local/bigstitcher_spark'

include {
    get_values_as_collection;
    is_local_file;
    param_as_file;
} from '../nfutils/utils'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    INPUT AND VARIABLES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

module = get_module(params.module)
module_params = params.module_params

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

    def ch_versions = Channel.empty()


    //
    // Create channel from params.output
    //

    def ch_data_inputs = Channel.of(params.output)
        .multiMap { o ->
            def meta = [ id: "bigstitcher" ]
            def data_files = []
            def module_args = []
            if (params.xml) {
                if (is_local_file(params.xml)) {
                    // only add it as a file if it's a local file
                    data_files << param_as_file(params.xml)
                }
                // xml inputs are always passed using '-x' flag
                module_args << '-x' << param_as_file(params.xml)
            }
            if (is_local_file(o)) {
                data_files << param_as_file(o)
            }
            // outputs are always passed using '-o' flag
            module_args << '-o' << param_as_file(o)

            if (module_params) {
                module_args << module_params
            }

            if (params.input_data_files) {
                get_values_as_collection(params.input_data_files).forEach { f ->
                    data_files << param_as_file(f)
                }
            }

            log.debug "BigStitcher input: ${meta}, ${module.module_class} ${module_args}, data files: ${data_files}"
            data_inputs: [ meta, data_files ]
            module_inputs: [ meta, module.module_class, module_args ]
        }

    BIGSTITCHER_SPARK(
        ch_data_inputs.data_inputs,
        ch_data_inputs.module_inputs,
        [:], // spark config
        params.distributed && module.parallelizable,
        file("${params.work_dir}/${workflow.sessionId}"),
        params.spark_workers,
        params.min_spark_workers,
        params.spark_worker_cpus,
        params.spark_mem_gb_per_cpu,
        params.spark_driver_cpus,
        params.spark_driver_mem_gb
    )

    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(
            storeDir: "${params.publishdir}/pipeline_info",
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
def get_module(module) {
    switch(module) {
        case 'create-container':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.CreateFusionContainer',
                parallelizable: false,
            ]
        case 'resave':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkResaveN5',
                parallelizable: true,
            ]
        case 'stitching':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkPairwiseStitching',
                parallelizable: true
            ]
        case 'affine-fusion':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkAffineFusion',
                parallelizable: true
            ]
        default:
            error "Unsupported module: ${module}"
    }
}