process DOWNLOAD {
    label 'process_single'

    input:
    tuple val(meta), val(url), path(dest)

    output:
    tuple val(meta), env(full_dest_path), emit: data

    when:
    task.ext.when == null || task.ext.when

    script:
    def destname = file(dest).name

    """
    full_dest_path=\$(readlink ${dest})
    if [[ ! -d "\${full_dest_path}" ]]; then
        mkdir -p "\${full_dest_path}"
    fi
    curl -skL "${url}" -o "${destname}.zip"
    unzip -o -d "\${full_dest_path}" "${destname}.zip"
    """
}
