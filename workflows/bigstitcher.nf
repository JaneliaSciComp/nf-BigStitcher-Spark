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

    take:
    ch_data // channel: bigstitcher jobs to run
    main:

    ch_data.subscribe { log.info "Input data $it" }

    ch_versions = Channel.empty()


    //
    // Create channel from input file and output dir provided through params.input and params.outdir
    //

    Channel.of([file(outdir)])
        .map { output_dir ->
            [ [id: outdir.name], xml_file, output_dir, module_class, module_params ]
        }
        .set { ch_data }


    // Parse parameters
    module_class = get_module_class(module)
    xml_file = xml ? file(xml) : null


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