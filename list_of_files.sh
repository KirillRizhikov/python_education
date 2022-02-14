#! /usr/bin/env bash

# Work with path
current_path=$PWD
echo
echo  Current path:
echo $current_path

# List of files
echo -e '\nList of files:'
find . -maxdepth 1 -type f

# Date
echo -e '\nCurrent date:'
current_date=`date`
echo "$current_date"

echo -e '\nGood luck!'
