#!/usr/bin/env python3

import subprocess

PARALLELISM = 1
JDBC_URL = "jdbc:postgresql://postgres:5432/twitch?user=postgres&password=password" 
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
KAFKA_TOPIC = "TwitchMessages"
AGGREGATION_INTERVAL_MS = 900000 # 15 min, event-time
TRIGGER_INTERVAL_MS = 5000 # 5 sec, processing-time

# Runs shell command, e.g. ['ls', '-l'] and redirect process stdout to console stdout
def run_shell_command(command_arr):
    p = subprocess.Popen(command_arr, shell=False)
    p.communicate()


print('Copying jar into container...')
run_shell_command(['docker', 'cp', './build/libs/twitch-chat-analyzer-1.1-SNAPSHOT.jar', 'tca_flink-jobmanager:/analyser.jar'])

print('Fixing permissions of data directories in container...')
run_shell_command(['docker', 'exec', '-u', 'root', 'tca_flink-jobmanager', 'mkdir', '-p', '/data/checkpoints'])
run_shell_command(['docker', 'exec', '-u', 'root', 'tca_flink-jobmanager', 'chown', 'flink:flink', '/data/checkpoints'])

print('Submitting job...')
run_shell_command(['docker', 'exec', 'tca_flink-jobmanager', 'flink', 'run', '-d', '-p', str(PARALLELISM), '-c', 'de.prkz.twitch.emoteanalyser.EmoteAnalyser', '/analyser.jar',
    JDBC_URL,
    KAFKA_BOOTSTRAP_SERVER,
    KAFKA_TOPIC,
    str(AGGREGATION_INTERVAL_MS),
    str(TRIGGER_INTERVAL_MS))
