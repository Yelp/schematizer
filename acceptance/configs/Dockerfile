FROM    busybox:latest
ADD     . /nail/srv/configs/
VOLUME  /nail/srv/configs/
VOLUME  /nail/etc/
VOLUME  /etc/boto_cfg/
ADD     nail-etc/services.yaml /nail/etc/services/services.yaml 
ADD     nail-etc/internal_ip_history.yaml /nail/srv/configs/internal_ip_history.yaml
ADD     nail-etc/yelp_profiling.yaml /etc/boto_cfg/yelp_profiling.yaml
ADD     nail-etc/runtimeenv /nail/etc/runtimeenv
VOLUME  /nail/etc/services
# safer_docker expects a command
CMD     echo
