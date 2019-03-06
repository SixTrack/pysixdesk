export PYSIXDESK_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "pysixdesk installed in $PYSIXDESK_ROOT"
export PYTHONPATH=${PYTHONPATH}:${PYSIXDESK_ROOT}/lib
