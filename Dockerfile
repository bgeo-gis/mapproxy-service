FROM sourcepole/qwc-uwsgi-base:ubuntu-v2023.05.12

RUN \
    apt-get update && \
    apt-get install -y libpq-dev gcc libgdal-dev

ADD ./requirements.txt /srv/qwc_service/requirements.txt

RUN pip3 install --no-cache-dir -r /srv/qwc_service/requirements.txt

ADD . /srv/qwc_service

# Fix for https://github.com/mapproxy/mapproxy/issues/558
RUN ln -s /usr/lib/libgdal.a /usr/lib/liblibgdal.a
