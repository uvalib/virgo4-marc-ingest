#if [ -z "$DOCKER_HOST" ]; then
#   echo "ERROR: no DOCKER_HOST defined"
#   exit 1
#fi

echo "*****************************************"
echo "running on $DOCKER_HOST"
echo "*****************************************"

if [ -z "$DOCKER_HOST" ]; then
   DOCKER_TOOL=docker
else
   DOCKER_TOOL=docker-legacy
fi

# set the definitions
INSTANCE=virgo4-marc-ingest
NAMESPACE=uvadave

$DOCKER_TOOL run -it $NAMESPACE/$INSTANCE /bin/bash -l

# return status
exit $?
