#/bin/bash

scope=$1
shift
query="$@ and namespace != $scope"
echo query: $query
metacat query -i $query > scope_${scope}.ids
