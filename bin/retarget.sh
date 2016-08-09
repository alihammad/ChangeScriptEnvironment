#/bin/bash

###########################################################################
#
#	This program can tokenize and de-tokenzie a single script (sql, bteq 
#	for example) or multiple scripts in a directory (recursively) based on 
#	the token values in the configuration file (../etc/tokenizer_replacement_values.ini)
#   Both configuration file and the source file/directory is provided 
# 	as a run-time argument to the program.
#
#	Tokenization Example:
#
#		script.sql
#		--------------
#		select * from mydatabase.mytable;
#		
#		Tokenization configuration file
#		-------------------------------
#		mydatabase=%mydbname%
#
#		Result script.sql_(timestamp)
#		-----------------------------
#		select * from %mydbname%.mytable;	
#
#	De-Tokenization (token replacement)Exampele:
#
#		De-Tokenization configuration file
#		----------------------------------
#		%mydbname%=mydatabase
#
#		Result script.sql_(timestamp)
#		-----------------------------
#		select * from mydatabase.mytable;	
#	
#	Usage:
#		retarget.sh\
# 		[ db file ] \
#		| [ from environment letter ] | [ to environment letter ] \
#		| [ [-f ] [ source file ] |  [-d ] [ source directory ] ]"
#	Options:
#
#		-f, --file				The source file
#		-d, --dir, --directory  The source directory
#		source file/directory	Path to file or directory
#
#	Revisions:
#
#	2016/05/11	File created
#
#	$Id: tokenization,v 1.0.1 2016/05/11 00:00:00 bshotts Exp $
###########################################################################


###########################################################################
#	Constants and Global Variables
###########################################################################

declare -r PROGNAME=$(basename "$0" | cut -d '.'  -f  1)
declare -r VERSION="1.0.1"
declare -r SCRIPTSHELL=${SHELL}

# Make some pretty date strings
declare -r DATE=$(date +'%m/%d/%Y')
declare -r YEAR=$(date +'%Y')
declare -r NOW_TS="$(date +%Y%m%d%H%M%S)"

# Get user's real name from passwd file
#declare -r AUTHOR=$(awk -v USER=$USER 'BEGIN { FS = ":" } $1 == USER { print $5 }' < /etc/passwd)

# Construct the user's email address from the hostname (this works in
# RH Linux, but not in Solaris) or the REPLYTO environment variable, if defined
#declare -r EMAIL_ADDRESS="<${REPLYTO:-${USER}@$(hostname)}>"

# Bring forth a few global variables
declare -r PURPOSE="(describe purpose of script)"
tokenize=

declare -r USAGE_MSG="\nUsage:
retarget.sh\
 [ db file ] \
| [ from environemnt letter ]  [ to environemnt letter ] \
| [ [-f ] [ source file ] |  [-d ] [ souerce directory ] ]\n"


# Check if required command line parameters are provided
if [[ "$#" -ne 5 ]]; then
	printf "\nError, not enough arguments.\n\n${USAGE_MSG}"  
	exit -1
fi

# get out of the bin dir
cd ..

declare -r root_dir="$(pwd)"
#declare -r suffix="$(date +%Y%m%d%H%M%S)"
#declare -r me="$(basename "$0" | cut -d '.'  -f  1)"
declare -r log_dir="${root_dir}/log"
declare -r log_file="${log_dir}/${PROGNAME}.log"
declare -r config_dir="${root_dir}/etc"
declare -r src="$5"
declare -r exempt_files=("*.sql" "*.bteq" "*.btq")
declare -r isfile=$([ "$4" = '-f' ] && echo true || echo false)
declare -r db_file="$1"
declare from_env="$2"
declare to_env="$3"
config_file=
declare tgt=

from_env=`echo $from_env | tr '[:lower:]' '[:upper:]'`
to_env=`echo $to_env | tr '[:lower:]' '[:upper:]'`

if [[ ${isfile} = true ]]; then
	tgt="${src}_${NOW_TS}"
else
	new_dir=$(echo $src | tr '/' '\n'| grep -v -e '^$' | tail -1)_${NOW_TS}
	tgt="${root_dir}/src/$new_dir"
fi

###########################################################################
#	Functions
###########################################################################

print_header() {
	printf "\n%s\n" "-----------------------------Execution started at ${NOW_TS}" | tee -a "${log_file}"
	printf "\nRoot directory: %s\n" "${root_dir}" | tee -a "${log_file}"
	printf "Log direcotry: %s\n" "${log_dir}" | tee -a "${log_file}"
	printf "Log file: %s\n" "${log_file}"| tee -a "${log_file}"
	printf "Source: %s\n" "${src}"| tee -a "${log_file}"
	printf "Target: %s\n" "${tgt}"| tee -a "${log_file}"
	printf "%s\n\n" "-----------------------------" | tee -a "${log_file}"
}

copy_src() {
	local -r in="$1";  shift
	local -r out="$1"; shift  

	printf "%s\n" "Copying ${in} to ${out}" "Please wait..." | tee  -a "${log_file}"
	cp -r "${in}" "${out}"
	if [[ $? -ne 0 ]]; then
		printf "%s\n" "source: ${in}" "target: ${out}"
		exit 
	fi
	printf "Copying done.\n\n" | tee  -a "${log_file}"
}

#######################################
# Remove all files from the target directory 
# except with following extensions
#	.sql
#	.bteq 
#	.btq
# Not applicable in case only a file is provided
#
# Globals:
#   None
# Arguments:
#   src source file/direcotry 
#	tgt target file/directory
# Returns:
#   None
#######################################
filter_source_scripts() {
	printf "Filtering files from target directory ${tgt}\n" | tee  -a "${log_file}"
	cd "${tgt}"
	find . ! -iname "${exempt_files[0]}" ! -name "${exempt_files[1]}" -type f -delete
 }


#######################################
# Change environment character in each database name
# provided in db file (run time argument) and store them
# in a temporary file
#
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
parse_db_list() {
	local -r from_env_letter="$1"; shift
	local -r to_env_letter="$1"; shift
	sed_ptrn="s/\(.\)\(${from_env_letter}\)\(.*\)/\1${to_env_letter}\3/ig"
	sed "${sed_ptrn}" "${db_file}" > "${config_dir}/tmp_${from_env_letter}_to_${to_env_letter}.txt"

	printf "\nFind configuration in file ${config_dir}/tmp_${from_env_letter}_to_${to_env_letter}.txt\n"
}

#######################################
# Generate a file that contains Sed replacement strings 
# for each database name created by function parse_db_list() 
# Each string in the file will look like following
#	s/\bSOCOMM\b/SPCOMM/ig
#   This Sed string will replace word SOCOMM with SPCOMM
#
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
generate_conf_file() {
	local -r from_env_letter="$1"; shift
	local -r to_env_letter="$1"; shift
	
	truncate -s 0 "${config_dir}/config.ini"
	
	while read -r -u 4 from_ptrn && read -r -u 5 to_ptrn; do
		# printf "From ptn: ${from_ptrn} To Ptn: ${to_ptrn}\n"	
		printf "%s\n" "s/\b${from_ptrn}\b/${to_ptrn}/ig" >> "${config_dir}/config.ini"
	done 4<"${db_file}" 5<"${config_dir}/tmp_${from_env_letter}_to_${to_env_letter}.txt" 

	config_file="${config_dir}/config.ini"
	printf "Tokenizing the scripts using configuration file: %s\n" \
		"${config_file}"| tee -a "${log_file}"	 
}
#######################################
#	DEPRECATED 
# Change config file based on wheter to 
# tokenize or detokenize source  
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
token_or_detoken() {
	if [[ "${tokenize}" == 't' ]]; then 
		config_file="${config_dir}/tokenizer_replacement_values.ini"
		printf "Tokenizing the scripts using configuration file: %s\n" \
		"${config_file}"| tee -a "${log_file}"	 
	else 	
		config_file="${config_dir}/detokenizer_replacement_values.ini"	
		printf "De-Tokenizing the scripts using configuration file: %s\n" \
			"${config_file}"| tee -a "${log_file}"
	fi
}

#######################################
# Run through each file in the target directory, 
# find and replace any instance of items found in 
# in config file (refer to function generate_conf_file() 
# for more details about config file) 
#
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
find_n_replace() {
	printf "%s\n" "Start: Replacing tokens in ${tgt} at $(date +%T)" \
		"Please wait..." | tee -a "${log_file}"
	printf "%s\n" "Modified following files" >>  "${log_file}"
	
	if [[ "${isfile}" == true ]]; then
		printf " replacing ...\nconfigfile: ${config_file} \nsrc: ${src}"
		sed -f "$config_file" "${src}" > "${tgt}"
		printf "%s\n" "${src}" >> "${log_file}"
		#mv "${src}2" "${src}"		
	else
		cd "${tgt}"
		find "${tgt}" -type f -name '*.*' -print0 | while IFS= read -r -d '' file; do
			printf "%s\n" "${file}" >> "${log_file}"
			sed -f "$config_file" "${file}" > "${file}2"
			mv "${file}2" "${file}"		
		done
	fi
	printf "%s\n" "" "Finished: Replacing tokens in ${tgt} at $(date +%T)" | tee -a "${log_file}"
}
  
###########################################################################
#	Main
###########################################################################

print_header

# Create copy of original scripts
copy_src "${src}" "${tgt}"

# # Remove all files except whats mentioned in variable $file_filter
if [[ "${isfile}" == false ]]; then
 	filter_source_scripts
fi

# #
parse_db_list "${from_env}" "${to_env}" 

generate_conf_file "${from_env}" "${to_env}" 

# Decision: Tokenize or De-Tokenize the scripts
# token_or_detoken

# Process: Replace the tokens with their values w.r.t configuration file $config_file
find_n_replace
