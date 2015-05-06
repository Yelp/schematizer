FROM    docker-dev.yelpcorp.com/lucid_yelp

ENV     DEBIAN_FRONTEND noninteractive

RUN     apt-get update && \
        apt-get install -y \
            python-pkg-resources \
            python-setuptools \
            python-virtualenv \
            python-pip \
            git

# uwsgi deps
RUN     apt-get install -y libyaml-0-2 libxml2 libpython2.6

# Add the service code
ADD     requirements.txt /code/requirements.txt
RUN     virtualenv /code/virtualenv_run
RUN     /code/virtualenv_run/bin/pip install \
            -i https://pypi.yelpcorp.com/simple \
            -r /code/requirements.txt

# Share the logging directory as a volume
RUN     mkdir /tmp/logs
VOLUME  /tmp/logs

ADD     . /code

WORKDIR /code
ENV     BASEPATH /code
CMD     PORT=8888 /code/virtualenv_run/bin/python /code/serviceinit.d/schematizer start-no-daemon
EXPOSE  8888
