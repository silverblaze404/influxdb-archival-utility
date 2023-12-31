import argparse
from cProfile import runctx
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import os

def tprint(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
    print(timestamp + message)

def create_influxdb_client(host, port, db):
    client = InfluxDBClient(host=host, port=port)
    client.switch_database(db)
    return client

def close_influxdb_client(client):
    client.close()

def get_eligible_shards_by_before(client, db, before):
    current_time = datetime.utcnow()
    cutoff_time = current_time - timedelta(minutes=before)
    query = 'SHOW SHARDS'
    result = client.query(query)
    shards = [shard for shard in result.get_points() if shard['database'] == db and (datetime.strptime(shard['expiry_time'], '%Y-%m-%dT%H:%M:%SZ') < cutoff_time)]
    return shards

def get_eligible_shards_by_shard_ids(client, db, shard_ids):
    query = 'SHOW SHARDS'
    result = client.query(query)
    shards = [shard for shard in result.get_points() if shard['database'] == db and int(shard['id']) in shard_ids]  
    return shards

def confirm_delete():
    confirmation = input("Are you sure you want to proceed with the delete operation? (yes/no): ")
    return confirmation.lower().strip() == "yes"

def delete_old_data(client, time_interval, measurements):
    if measurements == ['all']:
        measurements = [m['name'] for m in client.get_list_measurements()]

    tprint("deleting measurements {}".format(measurements))

    # Calculate the cutoff time
    current_time = datetime.utcnow()
    cutoff_time = current_time - timedelta(minutes=time_interval)

    for measurement in measurements:
        # Construct the delete query
        delete_query = f'DELETE FROM "{measurement}" WHERE time < \'{cutoff_time.isoformat()}Z\''
        # Execute the delete query
        client.query(delete_query)
    return None

def get_eligible_shards(client, db, before, shard):
    if before is None and shard is None:
        eligible_shards = get_eligible_shards_by_before(client, db, before)
    elif before is not None and shard is not None:
        eligible_shards = get_eligible_shards_by_shard_ids(client, db, shard)
    elif before is None and shard is not None:
        eligible_shards = get_eligible_shards_by_shard_ids(client, db, shard)
    else:
        eligible_shards = get_eligible_shards_by_before(client, db, before)

    return eligible_shards

def backup(host, port, db, shard_dir, before, shard, function_str, skip_function):
    client = create_influxdb_client(host,port,db)
    # if before is not None and before < 10080:
    #     raise ValueError("The 'before' argument in the backup command must not be less than 10080.")

    eligible_shards = get_eligible_shards(client, db, before, shard)

    actual_shard_list = [dir_name for dir_name in os.listdir(shard_dir)]
    tprint("shard id folders inside shard_dir is {}".format(actual_shard_list))
    shard_location_list = []
    for eligible_shard in eligible_shards:
        tprint("eligible_shard is {} ".format(eligible_shard))
        # shard_location = shard_dir + str(eligible_shard['id'])
        # shard_location_list.append(shard_location)
        if str(eligible_shard['id']) in actual_shard_list:
            shard_location = shard_dir + '/' + str(eligible_shard['id'])
            shard_location_list.append(shard_location)

    # Implement backup functionality here
    tprint("Backup function called")
    tprint(f"Host: {host}")
    tprint(f"Port: {port}")
    tprint(f"Database: {db}")
    tprint(f"Shard Directory: {shard_dir}")
    tprint(f"Before: {before}")
    tprint(f"Shard: {shard}")
    tprint(f"Function: {function_str}")
    tprint(f"Skip Function: {skip_function}")
    # Add your backup logic using the provided named arguments
    if not skip_function and function_str:
        # Pass the arguments to the function
        tprint("eligible shard location list is {}".format(shard_location_list))
        eval(function_str)(shard_location_list)
    else:
        tprint("eligible shard location list is {}".format(shard_location_list))

    close_influxdb_client(client)

def delete(host, port, db, before, measurements):
    client = create_influxdb_client(host,port,db)
    if before is not None and before < 131400: ## 3 months in minutes
        raise ValueError("The 'before' argument in the delete command must not be less than 131400.")
    
    if not confirm_delete():
        tprint("Delete operation cancelled.")
        return

    delete_old_data(client, before, measurements)
    # Implement delete functionality here
    tprint("Delete function called")
    tprint(f"Host: {host}")
    tprint(f"Port: {port}")
    tprint(f"Database: {db}")
    tprint(f"Before: {before}")
    tprint(f"Measurements: {measurements}")
    # Add your delete logic
    close_influxdb_client(client)



def custom_function(shard_location_list):
    # Implement custom function here
    tprint("Executing custom function")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["backup", "delete"], help="Command to execute: backup or delete")
    parser.add_argument("--host", type=str, required=True, help="Host name")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    parser.add_argument("--db", type=str, required=True, help="Database name")

    args, unknown_args = parser.parse_known_args()

    if args.command == "backup":
        backup_parser = argparse.ArgumentParser()
        backup_parser.add_argument("--shard_dir", type=str, help="Shard directory")
        backup_parser.add_argument("--before", type=int, default=10080, help="Integer value representing 'before' argument")
        backup_parser.add_argument("--shards", nargs='+', type=int, help="Integer values representing 'shards' argument")
        backup_parser.add_argument("--backup_function", required=True, help="String representing a function to be executed")
        backup_parser.add_argument("--dry_run", action="store_true", help="Skip execution of the specified function")
        backup_args = backup_parser.parse_args(unknown_args)

        backup(
            args.host,
            args.port,
            args.db,
            backup_args.shard_dir,
            backup_args.before,
            backup_args.shards,
            backup_args.backup_function,
            backup_args.dry_run,
        )

    elif args.command == "delete":
        delete_parser = argparse.ArgumentParser()
        delete_parser.add_argument("--force", action="store_true", required=True, help="representing 'force' argument")
        delete_parser.add_argument("--before", type=int, required=True, help="Integer value representing 'before' argument")
        delete_parser.add_argument("--measurements", nargs='+', type=str, required=True, help="Measurement names (comma-separated) or 'all'")
        delete_args = delete_parser.parse_args(unknown_args)
        delete(args.host, args.port, args.db, delete_args.before, delete_args.measurements)
    else:
        parser.error("Invalid command. Use 'backup' or 'delete'.")
