#!/bin/bash

# Prints list of all local branches, which doesn't have remote.
#
# If '-d' argument passed it tries to delete found branches.
# Note that foreceful delete is used - so ANY UNMERGED CHANGES WILL BE LOST!
#
# Note this script doesn't inspect current branch.

for branch in `git branch | grep -v "*"`; do
    git branch -r | grep "$branch" > /dev/null
    if [ "$?" ==  "1" ]; then
        if [ "$1" == "-d" ]; then
            git branch -D $branch
        else
            echo "$branch"
        fi;
    fi;
done;
