FROM golang:1.12

COPY ./install.sh /opt
RUN bash /opt/install.sh
ENV PATH=$PATH:$GOPATH/bin