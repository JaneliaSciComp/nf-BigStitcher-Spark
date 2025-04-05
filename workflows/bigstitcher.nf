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
            [ [id: "bigstitcher"], xml_file, output_dir, module_class, module_params ]
        }
        .set { ch_data }

    ch_data.subscribe { 
        log.info "Input data: $it" 
    }


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