FROM python:3

COPY [ "src", "/app" ]
COPY [ "cloudinventario", "/app" ]
COPY [ "requirements.txt", "/app" ]

WORKDIR "/app"
RUN [ "pip", "install", "-r", "requirements.txt" ]

# By default we wait for docker exec
CMD [ "tail", "-f", "/dev/null" ]
