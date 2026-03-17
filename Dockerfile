# ======================================
# Build Target: RHEL/Rocky/AlmaLinux 9
# ======================================

# rocky9
FROM rockylinux:9 AS builder

# pg16
ENV PG_VERSION=16


# Enable CRB repo
RUN dnf install -y dnf-plugins-core \
    && dnf config-manager --set-enabled crb \
    && dnf clean all


# Add postgres repo and install all dependencies
RUN dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
    && dnf -qy module disable postgresql \
    && dnf install -y \
       gcc \
       gcc-c++ \
       make \
       cmake \
       perl-IPC-Run \
       postgresql${PG_VERSION}-devel \
       openssl-devel \
       krb5-devel \
       redhat-rpm-config \
    && dnf clean all

WORKDIR /build
COPY project/ .

RUN export PATH="/usr/pgsql-${PG_VERSION}/bin:$PATH" \
    && rm -rf build \
    && mkdir build && cd build \
    && cmake .. \
    && make -j$(nproc) \
    && make install