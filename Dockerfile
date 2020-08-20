FROM modulator:latest

MAINTAINER Fenglin Chen <f73chen@uwaterloo.ca>

# packages should already be set up in modulator:latest
USER root

# move in the yaml to build modulefiles from
COPY bcl2fastq_recipe.yaml /modulator/code/gsi/recipe.yaml
COPY bcl2fastq2-v2.20.0.422-Linux-x86_64.rpm /build_files/bcl2fastq2-v2.20.0.422-Linux-x86_64.rpm
COPY bcl2fastq2-v2.18.0.12-Linux-x86_64.rpm /build_files/bcl2fastq2-v2.18.0.12-Linux-x86_64.rpm

# install the programs required for the yaml build
RUN apt-get -m update && apt-get install -y rpm2cpio cpio

# build the modules and set folder / file permissions
RUN ./build-local-code /modulator/code/gsi/recipe.yaml --initsh /usr/share/modules/init/sh --output /modules && \
	find /modules -type d -exec chmod 777 {} \; && \
	find /modules -type f -exec chmod 777 {} \;

# add the user
RUN groupadd -r -g 1000 ubuntu && useradd -r -g ubuntu -u 1000 ubuntu
USER ubuntu

# copy the setup file to load the modules at startup
COPY .bashrc /home/ubuntu/.bashrc

ENV BCL2FASTQ_JAIL_ROOT="/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-jail-3.0.0"
ENV BCL2FASTQ_ROOT="/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-2.20.0.422"
ENV PYTHON_ROOT="/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6"
ENV BARCODEX_ROOT="/modules/gsi/modulator/sw/Ubuntu18.04/barcodex-1.0.5"

ENV PATH="/modules/gsi/modulator/sw/Ubuntu18.04/barcodex-1.0.5/bin:/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6/bin:/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-2.20.0.422/bin:/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-jail-3.0.0/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV MANPATH="/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6/share/man:/usr/share/man:/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-jail-3.0.0/share/man"
ENV LD_LIBRARY_PATH="/modules/gsi/modulator/sw/Ubuntu18.04/barcodex-1.0.5/lib:/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6/lib:/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-2.20.0.422/lib"
ENV LD_RUN_PATH="/modules/gsi/modulator/sw/Ubuntu18.04/bcl2fastq-2.20.0.422/libexec"
ENV PKG_CONFIG_PATH="/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6/lib/pkgconfig"
ENV PYTHONPATH="/modules/gsi/modulator/sw/Ubuntu18.04/barcodex-1.0.5/lib/python3.6/site-packages:/modules/gsi/modulator/sw/Ubuntu18.04/python-3.6/lib/python3.6/site-packages"

CMD /bin/bash
