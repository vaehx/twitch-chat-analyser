<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;

class MainController implements ControllerProviderInterface
{
	const EXCLUDED_CHATTERS = ["nightbot", "notheowner", "_streamelements_", "scootycoolguy"];
	
	const EMOTES_TABLE = "emotes";
	const CHANNEL_STATS_TABLE = "channel_stats";
	const USER_STATS_TABLE = "user_stats";
	const EMOTE_STATS_TABLE = "emote_stats";
	const USER_EMOTE_STATS_TABLE = "user_emote_stats";

	const DEFAULT_SERIES_RESOLUTION = 1000; // sample points

	public function connect(SilexApplication $app)
	{
		$route = $app['controllers_factory'];
		$db = $app['db'];

		/**
		 * Emote statistics overview
		 */
		$route->get('/', function(Request $request) use($app, $db) {
			// Determine visualized window bounds
			$shownMinutes = $request->query->get('shownMinutes', 24 * 60);
			if ($shownMinutes <= 0)
				$shownMinutes = 1;

			$latestTimestamp = self::getCurrentTimestamp();
			$earliestTimestamp = $latestTimestamp - $shownMinutes * 60 * 1000;

			// Get emote statistics
			$emoteStats = [];
			$visualizedEmotes = ['moon2MLEM', 'moon2S', 'moon2A', 'moon2N', 'PogChamp'];
			$minEmoteOccurrences = PHP_INT_MAX;
			foreach ($visualizedEmotes as $emote)
			{
				$stmt = $db->query("SELECT timestamp, occurrences FROM ".self::EMOTE_STATS_TABLE." "
								. " WHERE emote='$emote' AND timestamp >= $earliestTimestamp ORDER BY timestamp ASC");
				if ($stmt === false)
					continue;

				if ($stmt->rowCount() > 0)
				{
					$emoteStats[$emote] = self::resampleTimeSeries($stmt->fetchAll(), 'occurrences', 150);
				}
				else
				{
					$emoteStats[$emote] = [
						['timestamp' => $earliestTimestamp, 'occurrences' => 0],
						['timestamp' => $latestTimestamp, 'occurrences' => 0]
					];
				}

				$firstOccurrences = $emoteStats[$emote][0]['occurrences'];
				if ($firstOccurrences < $minEmoteOccurrences)
					$minEmoteOccurrences = $firstOccurrences;
			}

			// Get total message count
			$stmt = $db->query("SELECT timestamp, message_count FROM channel_stats WHERE timestamp >= $earliestTimestamp AND timestamp <= $latestTimestamp ORDER BY timestamp ASC");
			$channelStats = self::resampleTimeSeries($stmt->fetchAll(), 'message_count', self::DEFAULT_SERIES_RESOLUTION);

			// Chatters active in selected time window
			$stmt = $db->query("SELECT DISTINCT username FROM ".self::USER_STATS_TABLE." WHERE timestamp >= $earliestTimestamp AND timestamp <= $latestTimestamp ORDER BY username ASC");
			$recentChatters = [];
			$recentChattersTotal = 0;
			while ($row = $stmt->fetch())
			{
				$recentChattersTotal++;
				if ($recentChattersTotal <= 25)
					$recentChatters[] = $row[0];
			}

			return $app['twig']->render('index.twig', [
				'shownMinutes' => $shownMinutes,
				'channelStats' => $channelStats,
				'emoteStats' => $emoteStats,
				'emoteStatsMinOccurrences' => $minEmoteOccurrences,
				'recentChatters' => $recentChatters,
				'recentChattersTotal' => $recentChattersTotal
			]);
		})->bind('index');

		/**
		 * Emotes leaderboard
		 */
		$route->get('/emotes', function(Request $request) use($app, $db) {
			// Real occurrences (including all chatters)
			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM ".self::EMOTE_STATS_TABLE.""
							. " GROUP BY emote ORDER BY MAX(occurrences) DESC");
			$emotes = [];
			while ($row = $stmt->fetch())
			{
				$emotes[$row['emote']] = [
					'real_occurrences' => $row['occurrences'],
					'occurrences' => 0,
					'standardDeviation' => null
				];
			}

			// Leaderboard without excluded chatters
			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. " WHERE username NOT IN " . self::getExcludedChattersTuple()
							. " GROUP BY emote ORDER BY MAX(occurrences) DESC");
			while ($row = $stmt->fetch())
			{
				$emote = $row['emote'];
				if (!isset($emotes[$emote]))
					$emotes[$emote] = ['real_occurrences' => 0];

				$emotes[$emote]['occurrences'] = $row['occurrences'];

				// Calculate standard deviation of emote usages by users
				/*$stmt2 = $db->query("SELECT MAX(occurrences) FROM ".self::USER_EMOTE_STATS_TABLE." WHERE emote='$emote' AND username != '_streamelements_' GROUP BY username ORDER BY MAX(occurrences)");
				$occurrences = array_map(function($row) { return $row[0]; }, $stmt2->fetchAll());
				$emotes[$emote]['standardDeviation'] = self::getDeviationFrom($occurrences, $emotes[$emote]['real_occurrences']);*/
				$emotes[$emote]['standardDeviation'] = 0;
			}

			// Sort by real occurrences
			$sortBy = $request->query->get('sortBy', 'real_occurrences');
			$sortDesc = !$request->query->has('sortAsc');
			uasort($emotes, function($a, $b) use($sortBy, $sortDesc) {
				if ($a[$sortBy] == $b[$sortBy])
					return 0;
				else if ($sortDesc)
					return ($a[$sortBy] < $b[$sortBy]) ? 1 : -1;
				else
					return ($a[$sortBy] < $b[$sortBy]) ? -1 : 1;
			});

			return $app['twig']->render('emotes.twig', [
				'emotes' => $emotes]);
		})->bind('emotes');

		/**
		 * User Leaderboard for an emote
		 */
		$route->get('/emote/{emote}', function($emote) use($app, $db) {
			$stmt = $db->query("SELECT emote FROM ".self::EMOTES_TABLE." WHERE emote='$emote' LIMIT 1");
			if ($stmt->rowCount() == 0)
				$app->abort(404, "Emote not found");
			
			$stmt = $db->query("SELECT timestamp, occurrences FROM ".self::EMOTE_STATS_TABLE." WHERE emote='$emote' ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();
			$minOccurrences = $stats[0]['occurrences'];
			$stats = self::resampleTimeSeries($stats, 'occurrences', self::DEFAULT_SERIES_RESOLUTION);

			// Get total occurrences
			$stmt = $db->query("SELECT SUM(occurrences) FROM ("
							. "   SELECT MAX(occurrences) AS occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. "   WHERE emote='$emote' AND username != '_streamelements_' GROUP BY username) a");
			$totalOccurences = $stmt->fetch()[0];

			// Leaderboard
			$stmt = $db->query("SELECT username, MAX(occurrences) AS occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. " WHERE emote='$emote' AND username NOT IN " . self::getExcludedChattersTuple()
							. " GROUP BY username ORDER BY MAX(occurrences) DESC LIMIT 1000");
			$leaderboard = [];
			while ($row = $stmt->fetch())
			{
				$row['percentage'] = 100.0 * ($row['occurrences'] / $totalOccurences);
				$leaderboard[] = $row;
			}

			return $app['twig']->render('emote.twig', [
				'emote' => $emote,
				'leaderboard' => $leaderboard,
				'stats' => $stats,
				'totalOccurrences' => $totalOccurences,
				'totalOccurrences2' => $stats[count($stats) - 1]['occurrences'],
				'minOccurrences' => $minOccurrences]);
		})->bind('emote');

		/**
		 * Per-user stats for an emote
		 */
		$route->get('/emote/{emote}/user/{username}', function($emote, $username) use($app, $db) {
			$stmt = $db->query("SELECT timestamp, occurrences FROM ".self::USER_EMOTE_STATS_TABLE." WHERE emote='$emote' AND username='$username' ORDER BY timestamp ASC");
			$stats = self::resampleTimeSeries($stmt->fetchAll(), 'occurrences', self::DEFAULT_SERIES_RESOLUTION);
			
			return $app['twig']->render('user_emote.twig', [
				'emote' => $emote,
				'username' => $username,
				'stats' => $stats]);
		})->bind('emote_user');

		/**
		 * Users leaderboard on total message count
		 */
		$route->get('/users', function() use($app, $db) {
			$excluded_users = ["nightbot"];
			
			$stmt = $db->query("SELECT username, MAX(message_count) AS message_count FROM ".self::USER_STATS_TABLE." GROUP BY username ORDER BY MAX(message_count) DESC LIMIT 50");
			$leaderboard = [];
			while ($row = $stmt->fetch())
			{
				$username = $row['username'];
				if (in_array($username, self::EXCLUDED_CHATTERS))
					continue;

				$leaderboard[] = ['username' => $row['username'], 'message_count' => $row['message_count']];
			}

			return $app['twig']->render('users.twig', ['users' => $leaderboard]);
		})->bind('users');

		/**
		 * User message count
		 */
		$route->get('/user/{username}', function($username) use($app, $db) {
			$windowStart = 0;
			$windowEnd = self::getCurrentTimestamp();
			
			$stmt = $db->query("SELECT timestamp, message_count FROM ".self::USER_STATS_TABLE.""
							. " WHERE username='$username' AND timestamp >= $windowStart AND timestamp <= $windowEnd"
							. " ORDER BY timestamp ASC");
			if ($stmt->rowCount() == 0)
				$app->abort(404, "User not found");

			$stats = self::resampleTimeSeries($stmt->fetchAll(), 'message_count', self::DEFAULT_SERIES_RESOLUTION);
			if ($windowStart == 0)
				$windowStart = $stats[0]['timestamp'];

			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. " WHERE username='$username' AND timestamp >= $windowStart AND timestamp <= $windowEnd"
							. " GROUP BY emote ORDER BY MAX(occurrences) DESC");
			$emoteUsages = [];
			while ($row = $stmt->fetch())
				$emoteUsages[$row['emote']] = $row['occurrences'];
			
			return $app['twig']->render('user.twig', [
				'username' => $username,
				'windowStart' => $windowStart,
				'windowEnd' => $windowEnd,
				'stats' => $stats,
				'emoteUsages' => $emoteUsages]);
		})->bind('user');



		$route->get('/dump/emotes', function() use($app, $db) {
			echo "<pre style='max-width: 100%; white-space: normal;'>";
			echo "INSERT INTO emotes(emote) VALUES";

			$stmt = $db->query("SELECT emote FROM emotes ORDER BY emote ASC");
			while ($row = $stmt->fetch()) {
				$emote = $row['emote'];
				echo "('$emote'), ";
			}

			echo "</pre>";
			return "";
		})->bind('dump_emotes');

		return $route;
	}

	// Returns array of (timestamp, occurrences) tuples sorted by timestamp for this emote
	static function getEmoteStatistics($emote, $db, $earliestTimestamp=1) {
		$sql = "SELECT timestamp, SUM(max) AS occurrences"
			. " FROM ("
			. "   SELECT t.timestamp, username, MAX(occurrences)"
			. "   FROM (SELECT DISTINCT timestamp FROM ".self::USER_EMOTE_STATS_TABLE." WHERE emote='$emote' AND timestamp >= $earliestTimestamp) t"
			. "   INNER JOIN (SELECT * FROM ".self::USER_EMOTE_STATS_TABLE." WHERE emote='$emote' AND username NOT IN " . self::getExcludedChattersTuple() . ") e"
			. "   ON e.timestamp <= t.timestamp"
			. "   GROUP BY t.timestamp, username) a"
			. " GROUP BY timestamp"
			. " ORDER BY timestamp, SUM(max);";
		
		$stats = [];
		$stmt = $db->query($sql);
		return $stmt->fetchAll();
	}

	static function getExcludedChattersTuple() {
		return "('" . implode("','", self::EXCLUDED_CHATTERS) . "')";
	}

	// Calculate factor describing the deviation from the given value
	static function getDeviationFrom($array, $val)
	{
		$n = count($array);
		if ($n <= 1)
			return null;

		// sum((val - x)^2) for each x in array
		$val = max($array);
		$variance = array_sum(array_map(function($x) use($val) { return pow($x, 2); }, $array)) / ($n - 1);
		return sqrt($variance);
	}

	static function getCurrentTimestamp()
	{
		return round(microtime(true) * 1000);
	}

	// Re-Samples the given time series to reduce or increase resolution in time domain
	// This function assumes that series is already sorted by timestamp and is in the following format:
	//	[['timestamp' => ..., $fieldName => ''], ...]
	// numPoints is the number of points of the output series
	static function resampleTimeSeries($series, $fieldName, $numPoints=1000, $startTime=null, $endTime=null)
	{
		if ($numPoints < 2)
			$numPoints = 2;

		$n = count($series);
		if ($n == 0)
		{
			if (is_null($startTime) || is_null($endTime))
				throw new \Exception("Cannot resample time series: Length = 0 and no start- and/or end-time given");

			return [
				['timestamp' => $startTime, $fieldName => 0],
				['timestamp' => $endTime, $fieldName => 0]
			];
		}

		$first = reset($series);
		$last = end($series);

		if (is_null($startTime))
			$startTime = $first['timestamp'];
		if (is_null($endTime))
			$endTime = $last['timestamp'];

		if ($endTime - $startTime == 0)
		{
			$endTime = $startTime + 1000 * 60;
			$numPoints = 2;
		}

		$t = $startTime;
		$t_step = ($endTime - $startTime) / ($numPoints - 1);
		$result = [];
		$prevBeforeIdx = 0;
		while ($t <= $endTime)
		{
			if ($t <= $first['timestamp'])
			{
				// Not enough data before the start of the series
				$result[] = ['timestamp' => $t, $fieldName => $first[$fieldName]];
			}
			else if ($t >= $last['timestamp'])
			{
				// Not enough data after the end of the series
				$result[] = ['timestamp' => $t, $fieldName => $last[$fieldName]];
			}
			else
			{
				// Find the elements immediately before and after t (or on-time)
				$before = null;
				$after = null;

				for ($i = $prevBeforeIdx; $i < $n - 1; ++$i)
				{
					$pt = $series[$i];
					if ($pt['timestamp'] > $t)
						break;

					$pt_next = $series[$i + 1];
					if ($pt_next['timestamp'] >= $t)
					{
						$before = $pt;
						$after = $pt_next;
						$prevBeforeIdx = $i;
						break;
					}
				}

				$k = ($t - $before['timestamp']) / ($after['timestamp'] - $before['timestamp']);
				$result[] = [
					'timestamp' => $t,
					$fieldName => $before[$fieldName] + $k * ($after[$fieldName] - $before[$fieldName])
				];
			}
			
			if ($t == $endTime)
				break;

			$t = ceil($t + $t_step);
			if ($t > $endTime)
				$t = $endTime;
		}

		return $result;
	}
}
