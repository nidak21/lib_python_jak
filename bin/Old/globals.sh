#!/bin/sh

# sh library of functions, and var settings useful in sh scripts.
# to use this, put the line:
#			. filename
# at the start of your shellscript - where "filename" is the name of this file
# May have to alter these functions for different shells (ksh vs. Bourne
#   for example). The idea is to encapsulate shell differences HERE, so
#   shell scripts do not have to change if we change shells.
# Note the Bourne shell (sh) does not have function local variables. So
#  be careful to use weird varnames in the functions, and unset them
#  at the end of the functions.

#######################################################################
# Revision History
# $Workfile$
# $Author$
# $Date$
# $Log$
#######################################################################

export EDITOR HOME PATH TERM

script=`basename $0`		# set short name of this script for errmsgs

# ----------------------------------------------------------------------------
homeof () {
    # print to stdout the full pathname of the home directory of the user
    #   whose login id is $1.
    # print nothing if the user is not known.
    homeof_home=`csh -f -c "echo ~$1" 2>&1`
    if [ `expr "$homeof_home" : "^[Uu]nknown user"` = "0" ];then # unknown user
	echo $homeof_home
    else			# unknown user
        echo
    fi
    unset homeof_home
}	# end function homeof
# ----------------------------------------------------------------------------
fileext () {
    # print file extension (the part after the last ".") of $1 to stdout
    expr "$1" : '.*\.\(.*\)'
}	# end fileext
# ----------------------------------------------------------------------------
filewoext () {
    # print file without the extension of $1 to stdout
    # Example: filewoext /usr/bin.foo/foo.blah   prints /usr/bin.foo/foo
    # Example: filewoext /usr/bin.foo/foo        prints /usr/bin.foo/foo
    expr "$1" : '\(.*\)\.[^/][^/]*$' '|' "$1"
}	# end filewoext
# ----------------------------------------------------------------------------
filepathhead () {
    # print $1 with final path component removed to stdout
    #  i.e. filepathhead /usr/bin/foo.c    prints   /usr/bin
    expr "$1" : '\(.*\)/'
}	# end filepathhead
# ----------------------------------------------------------------------------
errmsg () {
    # print args to stderr
    # if $1 = "-n", it will not print a newline character after the msg.
    echo "$@" >&2
}	# end errmsg
# ----------------------------------------------------------------------------
pause () {
    # print "press enter to continue message" and pause til user hits enter
    errmsg -n "(press enter to continue)"
    read pause_theans
    unset pause_theans
}	# end pause
#
#----------------------------------------------------------------------------
ask_r () {
    # usage:  ask_r [-nl] prompt [defaultanswer]
    # Asks user for a non-null (i.e. "required") answer.
    # Prompts user with 'prompt', reads their input. (prompts go to stderr)
    # If defaultanswer is specified, user can choose that by pressing enter.
    # Keeps asking til we get a non-null answer to the prompt.
    # -nl means prompt should end with newline so user response is on a clean
    #    line. (default is for user response to be on the same line)
    # Writes answer to stdout.
    if [ "$1" = "-nl" ];then		# -nl specified
	ask_rsameline=""		# no special arg for echo cmd
	shift				# throw -nl away
    else				# no -nl specified, use same line
	ask_rsameline="-n"		# arg for echo to keep on same line
    fi
    ask_rqans=""
    while [ -z "$ask_rqans" ];do
	if [ -z "$2" ];then		# no default answer
	    errmsg $ask_rsameline "$1 "
	    read ask_rqans
	else				# default answer specified
	    if [ "$ask_rsameline" = "" ];then # -nl specified (separate lines)
		errmsg "$1"
		errmsg "[$2]"
	    else			# no -nl specified (all one line)
		errmsg $ask_rsameline "$1 [$2] "
	    fi
	    read ask_rqans
	    ask_rqans="${ask_rqans:-$2}"
	fi
    done
    echo "$ask_rqans"
    unset ask_rsameline ask_rqans
    return
}	# end function ask_r
#
##----------------------------------------------------------------------------
ask () {
    # usage:  ask [-nl] prompt [defaultanswer]
    # Asks user for a possibly null answer.
    # Prompts user with 'prompt', reads their input. (prompts go to stderr)
    # If defaultanswer is specified, user can choose that by pressing enter.
    # -nl means prompt should end with newline so user response is on a clean
    #    line. (default is for user response to be on the same line)
    # Writes answer to stdout.
    if [ "$1" = "-nl" ];then		# -nl specified
	ask_sameline=""			# no special arg for echo cmd
	shift				# throw -nl away
    else				# no -n specified
	ask_sameline="-n"		# arg for echo to keep on same line
    fi
    ask_qans=""
    if [ -z "$2" ];then		# no default answer
	errmsg $ask_sameline "$1 "
	read ask_qans
    else				# default answer specified
	if [ "$ask_sameline" = "" ];then # -nl specified (separate lines)
	    errmsg "$1"
	    errmsg "[$2]"
	else				# no -nl specified (all one line)
	    errmsg $ask_sameline "$1 [$2] "
	fi
	read ask_qans
	ask_qans="${ask_qans:-$2}"
    fi
    echo "$ask_qans"
    unset ask_sameline ask_qans
    return
}	# end function ask
##----------------------------------------------------------------------------
ask_1let () {
    # usage:  ask_1let [-nl] prompt letters [defaultanswer]
    # Ask user for a single letter response.
    # Prompts user with 'prompt', reads their input. (prompts go to stderr)
    # Will not return until user's input is one of the specified 'letters'.
    # If defaultanswer is specified, user can choose that by pressing enter.
    # -nl means prompt should end with newline so user response is on a clean
    #    line. (default is for user response to be on the same line)
    # Writes answer to stdout.
    if [ "$1" = "-nl" ];then		# -nl specified
	ask_1let_sameline=""		# no special arg for echo cmd
	shift				# throw -nl away
    else				# no -nl specified
	ask_1let_sameline="-n"		# arg for echo to keep on same line
    fi
    ask_1let_qans=""
    while [ -z "$ask_1let_qans" ];do
	if [ -z "$3" ];then		# no default answer
	    errmsg $ask_1let_sameline "$1 "
	    read ask_1let_qans
	else				# default answer specified
	    if [ "$ask_1let_sameline" = "" ];then # -nl specified
						  #   (separate lines)
		errmsg "$1"
		errmsg "[$3]"
	    else			# no -nl specified (all one line)
		errmsg $ask_1let_sameline "$1 [$3] "
	    fi
	    read ask_1let_qans
	    ask_1let_qans="${ask_1let_qans:-$3}"
	fi
	if [ `expr match "$ask_1let_qans" "[$2]"` != 1 ];then	# no match
	    ask_1let_qans=""
	fi
    done
    echo "$ask_1let_qans"
    unset ask_1let_sameline ask_1let_qans
    return
}	# end function ask_1let
#
##----------------------------------------------------------------------------
#  functions below here are korn shell functions that have not been
#   translated to sh yet...
## ----------------------------------------------------------------------------
#function anyfiles {
#    # check if the reg expr in $1 matches any files.
#    # returncode= 0 if it matches, returncode=1 if no files match.
#    # we assume the filename expansion (set +f) is turned on.
#    # there must be a better way to do this!
#
#    typeset thematchedfiles=`echo $1`
#    if [ "$thematchedfiles" = "$1" ];then # if equal, then no matches
#	return 1
#    else
#	return 0
#    fi
#}	# end anyfiles 
#
## ----------------------------------------------------------------------------
#true_uid () {
#    # find the true userid of the currently running process, print that
#    #  userid to stdout
#
#    theuid="`/usr/bin/id`"	# output of id command
#    theuid=${theuid#*\(}	# strip off thru first "("
#    theuid=${theuid%%\)*}	# strip off from first ")" to end
#    echo $theuid
#    unset theuid
#}	# end true_uid
#
## ----------------------------------------------------------------------------
#function chk_dir_exists_f {
#    # check that $1 is an existing directory (fatal),
#    # print error msg to stderr and exit 5 (fatal error) if it does not exist
#    if [ ! -d "$1" ];then
#	print -u2 "${script}: directory '$1' does not exist."
#		    # for some reason using the alias errmsg here does not work
#	exit 5
#    fi
#}	# end chk_dir_exists_f
#
## ----------------------------------------------------------------------------
#function chk_file_exists {
#    # check that file $1 exists.
#    # print error msg to stderr if it does not exist.
#    # Return code
#    #	0	- file exists
#    #	1	- file does not exist (err msg printed)
#    if [ ! -f "$1" ];then
#	print -u2 "${script}: file '$1' does not exist."
#		    # for some reason using the alias errmsg here does not work
#	return 1
#    fi
#    return 0
#}	# end chk_file_exists
#
## ----------------------------------------------------------------------------
