FROM hysds/pge-base

MAINTAINER malarout "namrata.malarout@jpl.nasa.gov"
LABEL description="SNS ASF Handshaking"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/asf_sns_handshaking
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/asf_sns_handshaking

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
