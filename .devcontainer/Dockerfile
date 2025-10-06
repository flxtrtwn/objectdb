from python:latest
ARG USER
USER root
SHELL ["/bin/bash", "-euco", "pipefail"]
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Europe/Berlin \
    LANG=C.UTF-8
RUN echo $TZ > /etc/timezone \
    && apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata
RUN apt-get update && apt-get install -y --no-install-recommends \
#     apt-utils \
#     bash \
#     ca-certificates \
     curl \
#     file \
#     gettext \
     git \
#     git-lfs \
#     gnupg \
#     less \
#     lsb-release \
#     locales \
#     make \
     nano \
#     openssh-client \
#     openssh-server \
     sudo \
#     unzip \
#     vim \
#     wget \
#     xz-utils \
#     zip \
#     zsh \
#     x11-apps \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*
RUN useradd -s /bin/bash -U -m ${USER} && \
    echo ${USER} ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/${USER} && \
    chmod 0440 /etc/sudoers.d/${USER}
USER ${USER}
ENV USER_HOME=/home/${USER}
RUN chown -R ${USER}:${USER} ${USER_HOME}

USER ${USER}
ENV PATH=$PATH:${USER_HOME}/.local/bin
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
WORKDIR ${USER_HOME}
ENV USER=$USER
CMD ["/bin/bash"]
