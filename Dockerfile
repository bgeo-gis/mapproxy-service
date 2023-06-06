FROM sourcepole/qwc-uwsgi-base:ubuntu-v2023.05.12

RUN \
    apt-get update && \
    apt-get install -y libpq-dev gcc libgdal-dev

ADD . /srv/qwc_service

RUN pip3 install --no-cache-dir -r /srv/qwc_service/requirements.txt

# Fix for https://github.com/mapproxy/mapproxy/issues/558
RUN ln -s /usr/lib/libgdal.a /usr/lib/liblibgdal.a
