#!/bin/bash
/opt/cache/redis-2.6.16/src/redis-cli -h 10.1.1.26 -p 6379 slaveof 10.1.1.25 6379