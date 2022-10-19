#!/usr/bin/env python3

import subprocess

PARALLELISM = 4

JAR_FILE = './build/libs/twitch-chat-analyzer-SNAPSHOT-all.jar'
MAIN_CLASS = 'de.prkz.twitch.emoteanalyser.EmoteAnalyser'
PROPERTIES_FILE = './job.properties'
JOBMANAGER_SERVICE = 'flink-jobmanager'

# in container
CHECKPOINTS_DIR = '/data/checkpoints'


# Runs shell command, e.g. ['ls', '-l'] and redirect process stdout to console stdout
def run_shell_command(command_arr):
    p = subprocess.Popen(command_arr, shell=False)
    p.communicate()


print('Copying jar into container...')
run_shell_command(['docker', 'compose', 'cp', JAR_FILE, JOBMANAGER_SERVICE + ':/analyser.jar'])

print('Copying config into container...')
run_shell_command(['docker', 'compose', 'cp', PROPERTIES_FILE, JOBMANAGER_SERVICE + ':/analyser.properties'])

print('Fixing permissions of data directories in container...')
run_shell_command(['docker', 'compose', 'exec', '-T', '-u', 'root', JOBMANAGER_SERVICE, 'mkdir', '-p', CHECKPOINTS_DIR])
run_shell_command(['docker', 'compose', 'exec', '-T', '-u', 'root', JOBMANAGER_SERVICE, 'chown', 'flink:flink', CHECKPOINTS_DIR])

print('Submitting job...')
run_shell_command(['docker', 'compose', 'exec', '-T', JOBMANAGER_SERVICE, 'flink', 'run',
                   '-d',
                   '-p', str(PARALLELISM),
                   '-c', MAIN_CLASS, '/analyser.jar',
                   '/analyser.properties'])
