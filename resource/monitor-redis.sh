ALIVE=$(/opt/cache/redis-2.6.16/src/redis-cli -h 10.1.1.25 -p 6379 -a 123 PING)
echo $ALIVE
