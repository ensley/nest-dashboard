import argparse
import os
import time, traceback
from datetime import datetime

import requests
import psycopg2


DB_CONFIG = {
    'host': '',
    'dbname': '',
    'user': '',
    'password': ''
}


def insert_in_db(data, timestamp):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO thermostat (time_requested, time_inserted, last_connection, structure_id, device_id, name, name_long, label, where_id, where_name, locale, temperature_scale, is_online, hvac_state, hvac_mode, previous_hvac_mode, is_using_emergency_heat, has_fan, has_leaf, can_cool, can_heat, is_locked, sunlight_correction_enabled, sunlight_correction_active, fan_timer_active, fan_timer_timeout, fan_timer_duration, humidity, ambient_temperature_f, ambient_temperature_c, target_temperature_f, target_temperature_c, target_temperature_low_f, target_temperature_low_c, target_temperature_high_f, target_temperature_high_c, away_temperature_low_f, away_temperature_low_c, away_temperature_high_f, away_temperature_high_c, eco_temperature_low_f, eco_temperature_low_c, eco_temperature_high_f, eco_temperature_high_c, locked_temp_min_f, locked_temp_min_c, locked_temp_max_f, locked_temp_max_c, software_version)"
                "VALUES (%(timestamp)s, %(time_inserted)s, %(last_connection)s, %(structure_id)s, %(device_id)s, %(name)s, %(name_long)s, %(label)s, %(where_id)s, %(where_name)s, %(locale)s, %(temperature_scale)s, %(is_online)s, %(hvac_state)s, %(hvac_mode)s, %(previous_hvac_mode)s, %(is_using_emergency_heat)s, %(has_fan)s, %(has_leaf)s, %(can_cool)s, %(can_heat)s, %(is_locked)s, %(sunlight_correction_enabled)s, %(sunlight_correction_active)s, %(fan_timer_active)s, %(fan_timer_timeout)s, %(fan_timer_duration)s, %(humidity)s, %(ambient_temperature_f)s, %(ambient_temperature_c)s, %(target_temperature_f)s, %(target_temperature_c)s, %(target_temperature_low_f)s, %(target_temperature_low_c)s, %(target_temperature_high_f)s, %(target_temperature_high_c)s, %(away_temperature_low_f)s, %(away_temperature_low_c)s, %(away_temperature_high_f)s, %(away_temperature_high_c)s, %(eco_temperature_low_f)s, %(eco_temperature_low_c)s, %(eco_temperature_high_f)s, %(eco_temperature_high_c)s, %(locked_temp_min_f)s, %(locked_temp_min_c)s, %(locked_temp_max_f)s, %(locked_temp_max_c)s, %(software_version)s);",
                {**data, **{'timestamp': datetime.fromtimestamp(timestamp), 'time_inserted': datetime.now()}}
            )


def every(delay, task):
    next_time = time.time() // delay * delay + delay
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            task(next_time)
        except Exception:
            traceback.print_exc()
            # in production code you might want to have this instead of course:
            # logger.exception("Problem while executing repetitive task.")
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay


def process(timestamp):
    data = get_data(os.environ['THERMOSTAT_ID'], os.environ['ACCESS_TOKEN'])
    insert_in_db(data, timestamp)


def get_data(device_id, access_token):
    url = f"https://developer-api.nest.com/devices/thermostats/{device_id}"
    token = f"{access_token}"

    headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }

    response = requests.request("GET", url, headers=headers, allow_redirects=False)
    if response.status_code == 307:
        response = requests.request("GET", response.headers['Location'], headers=headers, allow_redirects=False)

    print(f'{time.strftime("%Y-%m-%d %I:%M:%S %p %Z")}: ambient temperature = {response.json()["ambient_temperature_f"]}°F')

    return response.json()


def collect_arguments():
    parser = argparse.ArgumentParser(description='Begin the data ingestion procedure')
    parser.add_argument('-H', '--host', help='database host')
    parser.add_argument('-d', '--dbname', help='database name')
    parser.add_argument('-u', '--user', help='database username')
    parser.add_argument('-p', '--password', help='database password')

    return parser.parse_args()


def config(args):
    DB_CONFIG.update({
        'host': args.host,
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password
    })


if __name__ == '__main__':
    args = collect_arguments()
    config(args)
    every(300, process)
