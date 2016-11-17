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
RUN     apt-get install -y libyaml-0-2 \
            libxml2 \
            libxml2-dev \
            libxslt1-dev \
            libpython2.7 \
            libmysqlclient-dev \
            libssl0.9.8 \
            m4 \
            python-dev

# Add the service code
ADD     requirements.txt /code/requirements.txt
RUN     virtualenv --python python2.7 /code/virtualenv_run

# TODO(DATAPIPE-1507|abrar): stop pointing to yelpcorp pypi package
RUN     /code/virtualenv_run/bin/pip install \
            -r /code/requirements.txt

# Share the logging directory as a volume
RUN     mkdir /tmp/logs && chown -R nobody /tmp/logs/
VOLUME  /tmp/logs

ADD     . /code

WORKDIR /code
ENV     BASEPATH /code
USER    nobody
CMD     /code/virtualenv_run/bin/python -m serviceinitd.schematizer
EXPOSE  8888
