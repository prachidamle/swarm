FROM docs/base:latest
MAINTAINER Mary Anthony <mary@docker.com> (@moxiegirl)

RUN svn checkout https://github.com/docker/docker/trunk/docs /docs/content/engine
RUN svn checkout https://github.com/docker/compose/trunk/docs /docs/content/compose
RUN svn checkout https://github.com/docker/machine/trunk/docs /docs/content/machine
RUN svn checkout https://github.com/docker/distribution/trunk/docs /docs/content/registry
RUN svn checkout https://github.com/docker/toolbox/trunk/docs /docs/content/toolbox
RUN svn checkout https://github.com/docker/kitematic/trunk/docs /docs/content/kitematic
RUN svn checkout https://github.com/docker/tutorials/trunk/docs /docs/content/tutorials
RUN svn checkout https://github.com/docker/opensource/trunk/docs /docs/content/opensource

ENV PROJECT=swarm
# to get the git info for this repo
COPY . /src

COPY . /docs/content/$PROJECT/
