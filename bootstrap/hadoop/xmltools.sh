#!/bin/bash

# Usage: property <xml-file> <name> <value>
function xmlproperty {
    local xmlFile=$1
    local name=$2
    local val=$3

    # Remove property if it exists already
    if xmlstarlet -q sel -t -m "/configuration/property/name[.='$name']" -v "." $xmlFile; then
        xmlstarlet ed -L -d "/configuration/property/name[.='$name']/.." $xmlFile
    fi

    xmlstarlet ed -L -s "/configuration" -t elem -n "property" $xmlFile
    xmlstarlet ed -L -s "/configuration/property[last()]" -t elem -n "name" -v "$2" $xmlFile
    xmlstarlet ed -L -s "/configuration/property[last()]" -t elem -n "value" -v "$3" $xmlFile
}
