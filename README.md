# influxdb-archival-utility
influxdb-archival-utility


# usage
python3 influxdb_archive_v3.py backup --host='localhost' --port=8086 --db='GreyOrange' --shard_dir='/Users/priyaranjan.m/Work/grey_orange/backups/ranjan' --before=2 --function="custom_function"

python3 influxdb_archive_v3.py backup --host='localhost' --port=8086 --db='GreyOrange' --shard_dir='/Users/priyaranjan.m/Work/grey_orange/backups/ranjan' --shards 13 14 15 --function="custom_function"


python3 influxdb_archive_v3.py delete --force --host='localhost' --port=8086 --db='GreyOrange' --before=131400 --measurements='all'

python3 influxdb_archive_v3.py delete --force --host='localhost' --port=8086 --db='GreyOrange' --before=131400 --measurements m1 m2 m3
