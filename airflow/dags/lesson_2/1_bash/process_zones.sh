#!/usr/bin/sh

echo "$startup_message";
curl -o /tmp/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv;
awk -F ',' '{print $4}' /tmp/taxi_zone_lookup.csv | sort | uniq;
