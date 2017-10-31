FROM hysds/pge-base

MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="PDL base image"

ARG git_oauth_token
ARG id
ARG gid

#Change UID and GID from build
USER root
RUN set -ex && \
  usermod -u ${id} ops && \
  groupmod -g ${gid} ops && \
  chown -R ops:ops /home/ops/ /data && \
  usermod -g ${gid} ops
 
USER ops

# copy packages
COPY . /home/ops/verdi/ops/asf_sns_handshaking
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/asf_sns_handshaking

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
