FROM    ubuntu:14.04

ENV     DEBIAN_FRONTEND noninteractive

RUN     apt-get update && \
        apt-get install -y \
            python2.7 \
            python-pkg-resources \
            python-setuptools \
            python-virtualenv \
            python-pip \
            curl \
            git

# uwsgi deps
RUN     apt-get install -y libyaml-0-2 libxml2 libpython2.7 libmysqlclient-dev libssl0.9.8

# Add the service code
ADD     requirements.txt /code/requirements.txt
ADD     requirements-internal.txt /code/requirements-internal.txt
RUN     virtualenv --python python2.7 /code/virtualenv_run
RUN     /code/virtualenv_run/bin/pip install \
            -i https://pypi.yelpcorp.com/simple/ \
            -r /code/requirements.txt \
            -r /code/requirements-internal.txt

# Share the logging directory as a volume
RUN     mkdir /tmp/logs && chown -R nobody /tmp/logs/
VOLUME  /tmp/logs

ADD     . /code

WORKDIR /code
ENV     BASEPATH /code
USER    nobody
CMD     PORT=8888 /code/virtualenv_run/bin/python /code/serviceinitd/internal_schematizer start-no-daemon
EXPOSE  8888
