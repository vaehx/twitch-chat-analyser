#!/usr/bin/env python3

import subprocess

PARALLELISM = 1


# Runs shell command, e.g. ['ls', '-l'] and redirect process stdout to console stdout
def run_shell_command(command_arr):
    p = subprocess.Popen(command_arr, shell=False)
    p.communicate()


print('Copying jar into container...')
run_shell_command(['docker', 'cp', './build/libs/twitch-chat-analyzer-1.1-SNAPSHOT-all.jar', 'tca_flink-jobmanager:/analyser.jar'])

print('Copying config into container...')
run_shell_command(['docker', 'cp', './job.properties', 'tca_flink-jobmanager:/analyser.properties'])

print('Fixing permissions of data directories in container...')
run_shell_command(['docker', 'exec', '-u', 'root', 'tca_flink-jobmanager', 'mkdir', '-p', '/data/checkpoints'])
run_shell_command(['docker', 'exec', '-u', 'root', 'tca_flink-jobmanager', 'chown', 'flink:flink', '/data/checkpoints'])

print('Submitting job...')
run_shell_command(['docker', 'exec', 'tca_flink-jobmanager', 'flink', 'run', '-d', '-p', str(PARALLELISM), '-c', 'de.prkz.twitch.emoteanalyser.EmoteAnalyser', '/analyser.jar',
    '/analyser.properties'])
