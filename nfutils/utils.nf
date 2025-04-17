def get_values_as_collection(values, value_separator=',') {
    if (values) {
        if (values instanceof Collection) {
            values
        } else {
            values.tokenize(value_separator)
        }
    } else {
        return []
    }
}

def is_local_file(f) {
	return f &&
		!f.startsWithIgnoreCase('s3://') &&
		!f.startsWithIgnoreCase('gs://') &&
	    !f.startsWithIgnoreCase('https://')
}

def param_as_file(f) {
	if (f.startsWithIgnoreCase('s3://') ||
		f.startsWithIgnoreCase('gs://') ||
		f.startsWithIgnoreCase('https://')) {
		f
	} else {
		file(f)
	}				
}
