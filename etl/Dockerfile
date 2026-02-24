FROM python:3.11-slim

WORKDIR /app

ENV ODBCINI=/etc/odbc.ini
ENV ODBCSYSINI=/etc
ENV ODBCINSTINI=odbcinst.ini

RUN apt-get update \
    && apt-get install -y --no-install-recommends unixodbc odbcinst libodbc2 libodbcinst2 unixodbc-dev build-essential g++ gzip libfbclient2 \
    && rm -rf /var/lib/apt/lists/*

COPY etl/odbc/OdbcFb-LIB-2.0.5.156.amd64.gz /tmp/OdbcFb-LIB-2.0.5.156.amd64.gz
RUN mkdir -p /tmp/odbc \
    && tar -xzf /tmp/OdbcFb-LIB-2.0.5.156.amd64.gz -C /tmp/odbc \
    && cp /tmp/odbc/libOdbcFb.so /usr/lib/libOdbcFb.so \
    && chmod 644 /usr/lib/libOdbcFb.so \
    && rm -rf /tmp/odbc /tmp/OdbcFb-LIB-2.0.5.156.amd64.gz

RUN if [ -f /usr/lib/x86_64-linux-gnu/libodbcinst.so.2 ] && [ ! -f /usr/lib/x86_64-linux-gnu/libodbcinst.so.1 ]; then \
      ln -s /usr/lib/x86_64-linux-gnu/libodbcinst.so.2 /usr/lib/x86_64-linux-gnu/libodbcinst.so.1; \
    fi

RUN if [ -f /usr/lib/x86_64-linux-gnu/libfbclient.so.2 ] && [ ! -f /usr/lib/x86_64-linux-gnu/libfbclient.so ]; then \
      ln -s /usr/lib/x86_64-linux-gnu/libfbclient.so.2 /usr/lib/x86_64-linux-gnu/libfbclient.so; \
    fi

COPY etl/odbc/odbc_init.sh /usr/local/bin/odbc_init.sh
RUN chmod +x /usr/local/bin/odbc_init.sh

COPY etl/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY etl /app/etl

ENTRYPOINT ["/usr/local/bin/odbc_init.sh"]
CMD ["python", "/app/etl/raw_sync.py"]
