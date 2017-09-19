FROM python:3.6-alpine
MAINTAINER oliver@app-workshop.de

# ARGs & ENVs
# -------------------------------------------------------
ENV APPLICATION_ENVIRONMENT Test

# dirs & volumes
# --------------------------------------------------------
RUN mkdir -p /usr/src/etl \
	&& mkdir -p /usr/log \
	&& mkdir -p /usr/src/etl/config/secrets/

WORKDIR /usr/src/etl

COPY . /usr/src/etl

VOLUME /usr/src/etl/
VOLUME /usr/log/
VOLUME /usr/src/etl/config/secrets/



# Install requirements
# ----------------------------------------------------------
COPY requirements.txt /usr/src/etl/
RUN pip install --no-cache-dir -r requirements.txt

# Start & Stop
# -----------------------------------------------------------
STOPSIGNAL SIGTERM
ENTRYPOINT ["python3", "runetl.py"]