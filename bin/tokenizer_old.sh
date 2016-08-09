#/bin/bash

###########################################################################
#
#	This program can tokenize and de-tokenzie a single script (sql, bteq 
#	for example) or multiple scripts in a directory (recursively) based on 
#	the token values in the configuration file (../etc/tokenizer_replacement_values.ini)
#   Both configuration file and the source file/directory is provided 
# 	as a run-time argument to the program.
#
#	Tokenization Exampele:
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
#	De-Tokenization (token replacemnent)Exampele:
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
#		tokenization.sh [ [ -t | --tokenize ] | [ -r | --rt | --replace ] ] 
#						[ [  -f | --file ] | [ -d | --dir | --directory ] ]
#						[ source file | source directory ]
#	Options:
#
#		-t, --tokenize			The source script is getting tokenized
#		-r, --rt, --replace		Tokens are getting replaced/de-tokinzed in source
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
declare -r AUTHOR=$(awk -v USER=$USER 'BEGIN { FS = ":" } $1 == USER { print $5 }' < /etc/passwd)

# Construct the user's email address from the hostname (this works in
# RH Linux, but not in Solaris) or the REPLYTO environment variable, if defined
declare -r EMAIL_ADDRESS="<${REPLYTO:-${USER}@$(hostname)}>"

# Bring forth a few global variables
declare -r PURPOSE="(describe purpose of script)"
tokenize=
isfile=
script_source=
config_file=

declare -r USAGE_MSG="\nUsage:
\ttokenization.sh [ [ -t | --tokenize ]\
| [ -r | --rt | --replace ] ] [ [  -f | --file ] \
| [ -d | --dir | --directory ] ] [ source file | source directory ]\n"


# Check if required command line parameters are provided
if [[ "$#" -ne 3 ]]; then
	printf "\nError, not enough arguments.\n\n${USAGE_MSG}"  
	exit -1
fi

# Process run-time arguments
while [[ $# > 1 ]]
do
	key="$1"
	case $key in
		-t|--tokenize)
			tokenize='t'
			shift # past argument
		;;
		-r|--rt|--replace)
			#detokenize
			tokenize='r'
			shift
		;;
		-f|--file)
			isfile=true
			script_source="$2"
			shift
		;;
		-d|--directory|--dir)
			isfile=false
			script_source="$2"
			shift
		;;
		*)
			#unknown option
			printf "Unknown option\n${USAGE_MSG}"
			exit -1
		;;
	esac
done

# get out of the bin dir
cd ..

declare -r root_dir="$(pwd)"
#declare -r suffix="$(date +%Y%m%d%H%M%S)"
#declare -r me="$(basename "$0" | cut -d '.'  -f  1)"
declare -r log_dir="${root_dir}/log"
declare -r log_file="${log_dir}/${PROGNAME}.log"
declare -r config_dir="${root_dir}/etc"
declare -r src="${script_source}"
declare -r exempt_files=("*.sql$", "*.log$")
declare declare tgt=

if [[ ${isfile} = true ]]; then
	tgt="${src}_${NOW_TS}"
else
	new_dir=$(echo $src | tr '/' '\n'| grep -v -e '^$' | tail -1)_${NOW_TS}
	tgt="${root_dir}/src/$new_dir"
fi

###########################################################################
#	Functions
###########################################################################

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

log() {
	echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&1
}

print_header() {
	printf "\n%s\n" "-----------------------------" 
	"Execution started at ${NOW_TS}" | tee -a "${log_file}"
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
# Remove all files except .sql and .bteq 
# files from copied source directory. Not 
# applicable incase only a file is provided
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
	find . -type f | egrep -v *.sql | sed 's/^/\"/g'| sed 's/$/\"/g' | xargs rm -rf	
 }

#######################################
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
# Add or replace tokens in the source file
# or source directory 
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

# Remove all files exept whats mentioned in varibale $exempt_files
if [[ "${isfile}" == false ]]; then
	filter_source_scripts
fi

# Decision: Tokenize or De-Tokenize the scripts
token_or_detoken

# Process: Replace the tokens with their values as w.r.t configuration file $config_file
find_n_replace
