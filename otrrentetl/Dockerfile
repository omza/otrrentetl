FROM omza/alpine-nginx-py3-supervisor
MAINTAINER oliver@app-workshop.de

# ARGs & ENVs
# -------------------------------------------------------
ENV NGINX_CONFIG /usr/src/app/deploy/nginx.prod.conf

ENV DIBOARDS_VERSION v0.1
ENV DIBOARDS_CONFIG_FILE /usr/src/app/config/default.py

# dirs & volumes
# --------------------------------------------------------
RUN mkdir -p /usr/src/app \
	&& mkdir -p /usr/db \
	&& mkdir -p /usr/log \
	&& mkdir -p /tmp \
	&& mkdir -p /usr/messages \
	&& mkdir -p /usr/qr \
	&& mkdir -p /secrets

WORKDIR /usr/src/app

COPY . /usr/src/app
RUN mv /usr/src/app/database/db.diboards /usr/db/db.diboards

VOLUME /usr/src/app/
VOLUME /usr/db/
VOLUME /usr/log/
VOLUME /secrets/
VOLUME /usr/messages/
VOLUME /usr/qr/


# Install requirements
# ----------------------------------------------------------
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# Start & Stop
# -----------------------------------------------------------
EXPOSE 80 443
STOPSIGNAL SIGTERM
ENTRYPOINT ["supervisord", "--nodaemon", "--configuration", "/usr/src/app/deploy/supervisord.conf"]