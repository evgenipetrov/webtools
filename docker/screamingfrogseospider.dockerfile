# Use the official Ubuntu 22.04 image as the base image
FROM ubuntu:22.04

ARG SEO_SPIDER_VERSION
ARG USER_NAME

ENV SEO_SPIDER_VERSION=$SEO_SPIDER_VERSION
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y wget xdg-utils zenity libgconf-2-4 libgtk2.0-0 libnss3 libxss1 chromium-browser
RUN echo ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true | debconf-set-selections

# Download Screaming Frog CLI using wget
RUN wget --no-verbose https://download.screamingfrog.co.uk/products/seo-spider/screamingfrogseospider_${SEO_SPIDER_VERSION}_all.deb

# Install Screaming Frog CLI using dpkg and fix any missing dependencies
RUN dpkg -i /screamingfrogseospider_${SEO_SPIDER_VERSION}_all.deb
RUN apt-get install -f -y

# Set the working directory for the container
WORKDIR /export

RUN useradd -ms /bin/bash $USER_NAME

# Copy configuration and license files to the appropriate directories
COPY secrets/spider.config /home/$USER_NAME/.ScreamingFrogSEOSpider/spider.config
COPY secrets/licence.txt /home/$USER_NAME/.ScreamingFrogSEOSpider/licence.txt

COPY secrets/listcrawl.seospiderconfig /seospiderconfig/listcrawl.seospiderconfig
COPY secrets/spidercrawl.seospiderconfig /seospiderconfig/spidercrawl.seospiderconfig

# Change ownership of the work directory & user home
RUN chown -R $USER_NAME:$USER_NAME /export
RUN chown -R $USER_NAME:$USER_NAME /home/$USER_NAME



USER $USER_NAME
ENTRYPOINT ["/usr/bin/screamingfrogseospider"]

# Set the default command to display help information
CMD ["--help"]