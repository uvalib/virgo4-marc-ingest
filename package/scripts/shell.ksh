if [ -z "$DOCKER_HOST" ]; then
   echo "ERROR: no DOCKER_HOST defined"
   exit 1
fi

echo "*****************************************"
echo "running on $DOCKER_HOST"
echo "*****************************************"

# set the definitions
INSTANCE=virgo4-marc-ingest
NAMESPACE=uvadave

docker run -it $NAMESPACE/$INSTANCE /bin/bash -l

# return status
exit $?
