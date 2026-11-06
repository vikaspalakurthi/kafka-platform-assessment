# Ansible modules need Python on the target, and the Apache image has none.
FROM apache/kafka:4.1.0

USER root
RUN apk add --no-cache python3 \
    && mkdir -p /etc/kafka /var/lib/kafka/data \
    && chown -R appuser:appuser /etc/kafka /var/lib/kafka
COPY --chmod=755 entrypoint.sh /usr/local/bin/lab-entrypoint.sh

USER appuser
# nc, not a Kafka CLI tool: a JVM every 10 seconds would be heavier than the check.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD nc -z 127.0.0.1 9092 || exit 1
ENTRYPOINT ["/usr/local/bin/lab-entrypoint.sh"]
