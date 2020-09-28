# Twitch Chat Analyser

## Setup

1. Build app: `mvn package`

2. Install dashboard dependencies: In `dashboard` directory, run `composer install`

3. Run docker setup: `docker-compose up -d`

4. Submit app with `submit.sh`

5. The app will periodically fetch subscriber emotes for all channels that at least one message was received for.

	* To add channels, update the `bot` section in `docker-compose.yml`.

	* To manually add emotes (e.g. non-subscriber emotes):

		```
		$ docker exec -ti tca_db psql -Upostgres -dtwitch
		twitch=# INSERT INTO channels(name) VALUES ('Kappa'), ('PogChamp'), ...;
		```

The Flink Web Panel will be available at `localhost:8081`. The dashboard is available at `localhost:8082`.


## Phrases

Phrases are configured in the `phrases` table and are matched case insensitive:

```
INSERT INTO phrases(name, regex) VALUES('furry', 'furr+y+');
```


## Processing a large batch of messages

To process historic messages you may want to load a large batch of messages into Kafka.
Since the current configuration is tuned for latency, processing of this batch may be slow.

To improve the performance of the "batch" processing, alter the configuration in `submit.sh` to your needs:
	
	* Increase database output batch size
	* Choose a longer trigger interval


## Troubleshooting

### Can't connect to port 8081 or 8082

Check your firewall settings for blocked INPUT routes or OUTPUT routes.

### Error: NoResourceAvailableException: Could not allocate all requires slots within timeout of 300000 ms

Check the "Available Task Slots" Flink Web Panel. By default, the app is configured to use parallelism 1, so you will need at least 4 available task slots. If none are available, make sure your Taskmanager container is running. 
