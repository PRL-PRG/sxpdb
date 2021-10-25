## VERSION = major.minor.patch
## Until major version is greater than 0, any changes in minor version number
## signifies version breaking change.
VERSION = "0.5.0"

## Primary Functionality

#' @export
# Open database specified by db
# If create is TRUE, then create the database.
# If create is FALSE, then load the database.
open_db <- function(db = "db", create = FALSE) {
	if (dir.exists(db)) {
		if (create) {
			stop(paste0(db, " already exists."))
		} else if (!file.exists(paste0(db, "/RECORD_VERSION"))) {
			stop(paste0(db, " is not a database."))
		} else {# valid database
			# Checking version
			f <- file(paste0(db, "/RECORD_VERSION"))
			v = readLines(f)
			if (v != VERSION) {
				stop("Version mismatch between database and program.")
			}

			close(f)

			# This must be called first
			.Call(RCRD_setup)

			# This must be called second
			.Call(RCRD_load_stats_store, paste0(db, "/stats.bin"))

			# Load Int Stores
			.Call(RCRD_load_int_index, paste0(db, "/int_index.bin"))
			.Call(RCRD_load_int_store, paste0(db, "/ints.bin"))
			.Call(RCRD_load_simple_int_store, paste0(db, "/s_ints.bin"))

			# Load Dbl Stores
			.Call(RCRD_load_dbl_index, paste0(db, "/dbl_index.bin"))
			.Call(RCRD_load_dbl_store, paste0(db, "/dbls.bin"))
			.Call(RCRD_load_simple_dbl_store, paste0(db, "/s_dbls.bin"))

			# Load Raw stores
			.Call(RCRD_load_raw_index, paste0(db, "/raw_index.bin"))
			.Call(RCRD_load_raw_store, paste0(db, "/raws.bin"))
			.Call(RCRD_load_simple_raw_store, paste0(db, "/s_raws.bin"))

			# Load Raw stores
			.Call(RCRD_load_str_index, paste0(db, "/str_index.bin"))
			.Call(RCRD_load_str_store, paste0(db, "/strs.bin"))
			.Call(RCRD_load_simple_str_index, paste0(db, "/s_str_index.bin"))
			.Call(RCRD_load_simple_str_store, paste0(db, "/s_strs.bin"))

			# Load Log store
			.Call(RCRD_load_log_index, paste0(db, "/log_index.bin"))
			.Call(RCRD_load_log_store, paste0(db, "/logs.bin"))

			# Load Cmp store
			.Call(RCRD_load_cmp_index, paste0(db, "/cmp_index.bin"))
			.Call(RCRD_load_cmp_store, paste0(db, "/cmps.bin"))

			# Load Lst store
			.Call(RCRD_load_lst_index, paste0(db, "/lst_index.bin"))
			.Call(RCRD_load_lst_store, paste0(db, "/lsts.bin"))

			# Load Env store
			.Call(RCRD_load_env_index, paste0(db, "/env_index.bin"))
			.Call(RCRD_load_env_store, paste0(db, "/envs.bin"))

			# Load Env store
			.Call(RCRD_load_fun_index, paste0(db, "/fun_index.bin"))
			.Call(RCRD_load_fun_store, paste0(db, "/funs.bin"))

			# Load generic store
			.Call(RCRD_load_generic_index, paste0(db, "/generic_index.bin"))
			.Call(RCRD_load_generic_store, paste0(db, "/generics.bin"))
		}
	} else {
		if (!create) {
			stop(paste0(db, " does not exist."))
		} else {
			dir.create(db, recursive = TRUE)

			# Create version file
			version_file = paste0(db, "/RECORD_VERSION")

			# Create the stats store file
			stats = paste0(db, "/stats.bin")

			# Create Int Stores
			ints = paste0(db, "/ints.bin")
			int_index = paste0(db, "/int_index.bin")
			s_ints = paste0(db, "/s_ints.bin")

			# Create Dbl Stores
			dbls = paste0(db, "/dbls.bin")
			dbl_index = paste0(db, "/dbl_index.bin")
			s_dbls = paste0(db, "/s_dbls.bin")

			# Create Raw Stores
			raws = paste0(db, "/raws.bin")
			raw_index = paste0(db, "/raw_index.bin")
			s_raws = paste0(db, "/s_raws.bin")

			# Create Str Stores
			strs = paste0(db, "/strs.bin")
			str_index = paste0(db, "/str_index.bin")
			s_strs = paste0(db, "/s_strs.bin")
			s_str_index = paste0(db, "/s_str_index.bin")

			# Create Log Store
			logs = paste0(db, "/logs.bin")
			log_index = paste0(db, "/log_index.bin")

			# Create Cmp Store
			cmps = paste0(db, "/cmps.bin")
			cmp_index = paste0(db, "/cmp_index.bin")

			# Create Lst Store
			lsts = paste0(db, "/lsts.bin")
			lst_index = paste0(db, "/lst_index.bin")

			# Create Env Store
			envs = paste0(db, "/envs.bin")
			env_index = paste0(db, "/env_index.bin")

			# Create Fun Store
			funs = paste0(db, "/funs.bin")
			fun_index = paste0(db, "/fun_index.bin")

			# Create the generic store files
			generics = paste0(db, "/generics.bin")
			generic_index = paste0(db, "/generic_index.bin")

			file.create(version_file,
						ints,
						int_index,
						s_ints,
						dbls,
						dbl_index,
						s_dbls,
						raws,
						raw_index,
						s_raws,
						strs,
						str_index,
						s_strs,
						s_str_index,
						logs,
						log_index,
						cmps,
						cmp_index,
						lsts,
						lst_index,
						envs,
						env_index,
						funs,
						fun_index,
						generics,
						generic_index,
						stats,
						showWarnings = TRUE)

			# This must be called first
			.Call(RCRD_setup)

			# This must be called second
			.Call(RCRD_init_stats_store, stats)

			# Initialize Int Stores
			.Call(RCRD_init_int_index, int_index)
			.Call(RCRD_init_int_store, ints)
			.Call(RCRD_init_simple_int_store, s_ints)

			# Initialize Dbl Stores
			.Call(RCRD_init_dbl_index, dbl_index)
			.Call(RCRD_init_dbl_store, dbls)
			.Call(RCRD_init_simple_dbl_store, s_dbls)

			# Initialize Raw Stores
			.Call(RCRD_init_raw_index, raw_index)
			.Call(RCRD_init_raw_store, raws)
			.Call(RCRD_init_simple_raw_store, s_raws)

			# Initialize Str Stores
			.Call(RCRD_init_str_index, str_index)
			.Call(RCRD_init_str_store, strs)
			.Call(RCRD_init_simple_str_index, s_str_index)
			.Call(RCRD_init_simple_str_store, s_strs)

			# Initialize log store
			.Call(RCRD_init_log_index, log_index)
			.Call(RCRD_init_log_store, logs)

			# Initialize cmp store
			.Call(RCRD_init_cmp_index, cmp_index)
			.Call(RCRD_init_cmp_store, cmps)

			# Initialize lst store
			.Call(RCRD_init_lst_index, lst_index)
			.Call(RCRD_init_lst_store, lsts)

			# Initialize env store
			.Call(RCRD_init_env_index, env_index)
			.Call(RCRD_init_env_store, envs)

			# Initialize fun store
			.Call(RCRD_init_fun_index, fun_index)
			.Call(RCRD_init_fun_store, funs)

			# Initialize generic store
			.Call(RCRD_init_generic_index, generic_index)
			.Call(RCRD_init_generic_store, generics)

			# Initialize version file
			f <- file(version_file)
			writeLines(VERSION, version_file)
			close(f)
		}
	}
}

#' @export
close_db <- function() {
	# Close generic store
	.Call(RCRD_close_generic_index)
	.Call(RCRD_close_generic_store)

	# Close Int Stores
	.Call(RCRD_close_int_index)
	.Call(RCRD_close_int_store)
	.Call(RCRD_close_simple_int_store)

	# Close Dbl Stores
	.Call(RCRD_close_dbl_index)
	.Call(RCRD_close_dbl_store)
	.Call(RCRD_close_simple_dbl_store)

	# Close Raw store
	.Call(RCRD_close_raw_index)
	.Call(RCRD_close_raw_store)
	.Call(RCRD_close_simple_raw_store)

	# Close String store
	.Call(RCRD_close_str_index)
	.Call(RCRD_close_str_store)
	.Call(RCRD_close_simple_str_index)
	.Call(RCRD_close_simple_str_store)

	# Close Log store
	.Call(RCRD_close_log_index)
	.Call(RCRD_close_log_store)

	# Close Cmp store
	.Call(RCRD_close_cmp_index)
	.Call(RCRD_close_cmp_store)

	# Close Lst store
	.Call(RCRD_close_lst_index)
	.Call(RCRD_close_lst_store)

	# Close Env store
	.Call(RCRD_close_env_index)
	.Call(RCRD_close_env_store)

	# Close Fun store
	.Call(RCRD_close_fun_index)
	.Call(RCRD_close_fun_store)

	# This must be called second to last
	.Call(RCRD_close_stats_store)

	# This must be called last
	.Call(RCRD_teardown)
}

#' @export
add_val <- function(val) {
	.Call(RCRD_add_val, val)
}

#' @export
sample_val <- function(type = "any") {
	if (type == "any") {
		.Call(RCRD_sample_val)
	} else if (type == "null") {
		.Call(RCRD_sample_null)
	} else if (type == "integer") {
		.Call(RCRD_sample_int)
	} else if (type == "double") {
		.Call(RCRD_sample_dbl)
	} else if (type == "raw") {
		.Call(RCRD_sample_raw)
	} else if (type == "string") {
		.Call(RCRD_sample_str)
	} else if (type == "logical") {
		.Call(RCRD_sample_log)
	} else if (type == "complex") {
		.Call(RCRD_sample_cmp)
	} else if (type == "list") {
		.Call(RCRD_sample_lst)
	} else if (type == "environment") {
		.Call(RCRD_sample_env)
	} else if (type == "function") {
		.Call(RCRD_sample_fun)
	} else if (type == "generic") {
		.Call(RCRD_sample_generic)
	} else {
		stop("Trying to sample unknown type")
	}
}

#' @export
merge_db <- function(other_db = "db") {
	# TODO: Create a better way to see if we are working with a record db
	# TODO: Create a better way to see if we are working with another record db
	if (dir.exists(other_db) &&
		# TODO: Change this to check for version
		file.exists(paste0(other_db, "/generics.bin"))) {

		# Create Int Store Names
		ints = paste0(other_db, "/ints.bin")
		int_index = paste0(other_db, "/int_index.bin")
		s_ints = paste0(other_db, "/s_ints.bin")

		# Create Dbl Stores Names
		dbls = paste0(other_db, "/dbls.bin")
		dbl_index = paste0(other_db, "/dbl_index.bin")
		s_dbls = paste0(other_db, "/s_dbls.bin")

		# Create Raw Stores Names
		raws = paste0(other_db, "/raws.bin")
		raw_index = paste0(other_db, "/raw_index.bin")
		s_raws = paste0(other_db, "/s_raws.bin")

		# Create Str Stores Names
		strs = paste0(other_db, "/strs.bin")
		str_index = paste0(other_db, "/str_index.bin")
		s_strs = paste0(other_db, "/s_strs.bin")
		s_str_index = paste0(other_db, "/s_str_index.bin")

		# Create Log Store Names
		logs = paste0(other_db, "/logs.bin")
		log_index = paste0(other_db, "/log_index.bin")

		# Create Cmp Store Names
		cmps = paste0(other_db, "/cmps.bin")
		cmp_index = paste0(other_db, "/cmp_index.bin")

		# Create Lst Store Names
		lsts = paste0(other_db, "/lsts.bin")
		lst_index = paste0(other_db, "/lst_index.bin")

		# Create Env Store Names
		envs = paste0(other_db, "/envs.bin")
		env_index = paste0(other_db, "/env_index.bin")

		# Create Fun Store Names
		funs = paste0(other_db, "/funs.bin")
		fun_index = paste0(other_db, "/fun_index.bin")

		# Create Generic Store Names
		generics = paste0(other_db, "/generics.bin")
		generic_index = paste0(other_db, "/generic_index.bin")

		# Create Stats Store Names
		stats = paste0(other_db, "/stats.bin")

		# Merge Ints
		.Call(RCRD_merge_int_store, ints, int_index)
		.Call(RCRD_merge_simple_int_store, s_ints)

		# Merge Dbls
		.Call(RCRD_merge_dbl_store, dbls, dbl_index)
		.Call(RCRD_merge_simple_dbl_store, s_dbls)

		# Merge Raws
		.Call(RCRD_merge_raw_store, raws, raw_index)
		.Call(RCRD_merge_simple_raw_store, s_raws)

		# Merge Strs
		.Call(RCRD_merge_str_store, strs, str_index)
		.Call(RCRD_merge_simple_str_store, s_strs, s_str_index)

		# Merge Logs
		.Call(RCRD_merge_log_store, logs, log_index)

		# Merge Cmps
		.Call(RCRD_merge_cmp_store, cmps, cmp_index)

		# Merge Lsts
		.Call(RCRD_merge_lst_store, lsts, lst_index)

		# Merge Envs
		.Call(RCRD_merge_env_store, envs, env_index)

		# Merge Envs
		.Call(RCRD_merge_fun_store, funs, fun_index)

		# Merge Rest
		.Call(RCRD_merge_generic_store, generics, generic_index)
		.Call(RCRD_merge_stats_store, stats)
	}
}

## Testing/Information Gathering Related Functionality

#' @export
have_seen <- function(val) {
	.Call(RCRD_have_seen, val)
}

#' @export
report <- function() {
	.Call(RCRD_print_report)
}

#' @export
print_vals <- function () {
	if (size_db()) {
		for(i in 0:(size_db() - 1)) {
			print(.Call(RCRD_get_val, i))
		}
	} else {
		stop("There are no values in the database.")
	}
}

#' @export
view_db <- function() {
	if(size_db() == 0) {
		viewer <- list()
	} else {
		viewer <- lapply(seq(from=0, to=size_db()-1), function(i) .Call(RCRD_get_val, i))
	}
	viewer
}

#' @export
count_vals <- function() {
	.Call(RCRD_count_vals)
}

#' @export
size_db <- function() {
	.Call(RCRD_size_db)
}

#' @export
size_ints <- function() {
	.Call(RCRD_size_ints)
}
