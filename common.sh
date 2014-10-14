#!/bin/bash

Remove() {
    #remove if exist
    if [ $1 ];then
        if [ -e $1 ];then
            echo "Delete $1"
            rm -f $1
        fi
    fi
}

Empty() {
    #0 empty. 1 not empty.
    if [ $1 ];then
        if [ -s $1 ];then
            return 1
        fi
    fi
    return 0
}

GetConfig() {
    #1 item, 2 key, 3 dir
    if [ $1 ] && [ $2 ] && [ $3 ];then
        awk -F '=' '/\['"$1"'\]/{a=1}a==1&&$1~/'"$2"'/{print $2;exit}' $3
    fi
}
