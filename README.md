# Twitch Chat Analyser

## Setup

1. Build app: `mvn package`

2. Install dashboard dependencies: In `dashboard` directory, run `composer install`

3. Run docker setup: `docker-compose up -d`

4. Submit app with `submit.sh`

5. After the first start, no emotes will be tracked. You can add them to the `emote` table in the Postgres DB. The app will reload this emote list every 60 seconds:

	1. Determine container id of the postgres db container (`docker ps`)

	2. Start a shell in this container: `docker exec -ti <db-container-id> bash`

	3. Start a postgre shell: `psql -Upostgres -dtwitch`

	4. Insert your emotes:
	
		```sql
		INSERT INTO emotes(emote) VALUES ('Kappa'), ('PogChamp'), ...;
		```

The Flink Web Panel will be available at `localhost:8081`. The dashboard is available at `localhost:8082`.

## Troubleshooting

### Can't connect to port 8081 or 8082

Check your firewall settings for blocked INPUT routes or OUTPUT routes.

### Error: NoResourceAvailableException: Could not allocate all requires slots within timeout of 300000 ms

Check the "Available Task Slots" Flink Web Panel. By default, the app is configured to use parallelism 1, so you will need at least 4 available task slots. If none are available, make sure your Taskmanager container is running. 
