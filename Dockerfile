FROM sourcepole/qwc-uwsgi-base:ubuntu-v2023.05.12

RUN \
    apt-get update && \
    apt-get install -y libpq-dev gcc libgdal-dev

ADD ./requirements.txt /srv/qwc_service/requirements.txt

RUN pip3 install --no-cache-dir -r /srv/qwc_service/requirements.txt

ENV UWSGI_PROCESSES=4
ENV UWSGI_THREADS=2
ENV UWSGI_EXTRA="--touch-reload /srv/touch_reload"

ADD . /srv/qwc_service
