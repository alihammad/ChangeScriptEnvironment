Usage

sh tokenization.sh [t|d] [f|d] [source file | source directory]
	[t|d]: tokenize |detokeniz
	[f|d]: file | directory

	t: tokenize file/s based on configuration file. Find the tokens(keys) in the configuration file and search it in 
	    all files/folders and replace all instances of that key with the value of that particular key in the configuration file.
	
	Configuration files are placed in "etc" folder
	
	d: de-tokenize file/s based on configuration file. 
	
	