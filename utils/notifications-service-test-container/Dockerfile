FROM alpine:latest

# Copy your script or tool into the container
RUN apk add --no-cache curl

COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p /output

CMD ["/usr/local/bin/entrypoint.sh"]