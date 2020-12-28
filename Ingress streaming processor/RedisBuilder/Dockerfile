FROM redis
RUN mkdir -p /var/log/redis /var/db/redis/
RUN chown -R redis:redis /var/log/redis /var/db/redis
COPY redis.conf /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]