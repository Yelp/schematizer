FROM    busybox:latest
ADD     . /nail/srv/configs/
VOLUME  /nail/srv/configs/
ADD     nail-etc/services.yaml /nail/etc/services/services.yaml
VOLUME  /nail/etc/services
# safer_docker expects a command
CMD     echo
