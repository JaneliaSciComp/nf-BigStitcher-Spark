include { DOWNLOAD          } from '../modules/local/download/main'
include { BIGSTITCHER_SPARK } from '../subworkflows/local/bigstitcher_spark'
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

    def bigstitcher_meta = [ id: "bigstitcher" ]
    def ch_data_inputs
    if (params.download_url) {
        // download the data first
        ch_data_inputs = Channel.of(params.download_url)
            | map { url ->
                log.debug "Download data from ${url}"
                if (!params.download_dir) {
                    error "Download directory not specified. Please set the download_dir parameter."
                }
                [ bigstitcher_meta, url, file(params.download_dir) ]
            }
            | DOWNLOAD
    } else {
        // input channel only has the metadata
        ch_data_inputs = Channel.of([ bigstitcher_meta ])
    }

    def bigstitcher_inputs = ch_data_inputs
        .multiMap {
            def (meta, downloaded_data_dir) = it
            def data_files = []
            def module_args = []
            if (downloaded_data_dir) {
                data_files << downloaded_data_dir
            }
            if (params.xml) {
                if (is_local_file(params.xml)) {
                    // only add it as a file if it's a local file
                    data_files << param_as_file(params.xml).parent
                }
                // xml inputs are always passed using '-x' flag
                module_args << '-x' << param_as_file(params.xml)
            }

            if (params.output) {
                if (is_local_file(o)) {
                    data_files << param_as_file(params.output).parent
                }
                // outputs are always passed using '-o' flag
                module_args << '-o' << param_as_file(o)
            }

            // xml output
            if (params.xmlout) {
                if (is_local_file(params.xmlout)) {
                    // add the parent file because the output does not exist
                    // so an attempt to mount it would result in an error
                    data_files << param_as_file(params.xmlout).parent
                }
                module_args << '-xo' << param_as_file(params.xmlout)
            }

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
        bigstitcher_inputs.data_inputs,
        bigstitcher_inputs.module_inputs,
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
        case 'affine-fusion':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkAffineFusion',
                parallelizable: true
            ]
        case 'clear-interestpoints':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.ClearInterestPoints',
                parallelizable: true,
            ]
        case 'clear-registrations':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.ClearRegistrations',
                parallelizable: true,
            ]
        case 'create-container':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.CreateFusionContainer',
                parallelizable: false,
            ]
        case 'detect-interestpoints':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkInterestPointDetection',
                parallelizable: true,
            ]
        case 'downsample':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkDownsample',
                parallelizable: true,
            ]
        case 'match-interestpoints':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkGeometricDescriptorMatching',
                parallelizable: true,
            ]
        case 'nonrigid-fusion':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkNonRigidFusion',
                parallelizable: true
            ]
        case 'resave':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkResaveN5',
                parallelizable: true,
            ]
        case 'solver':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.Solver',
                parallelizable: true
            ]
        case 'stitching':
            return [
                module_class: 'net.preibisch.bigstitcher.spark.SparkPairwiseStitching',
                parallelizable: true
            ]
        default:
            error "Unsupported module: ${module}"
    }
}
