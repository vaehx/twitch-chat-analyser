<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;

class MainController implements ControllerProviderInterface
{
	const EXCLUDED_CHATTERS = ["nightbot", "notheowner", "scootycoolguy"];

	const EMOTES_TABLE = "emotes";
	const CHANNEL_STATS_TABLE = "channel_stats";
	const USER_STATS_TABLE = "user_stats";
	const EMOTE_STATS_TABLE = "emote_stats";
	const USER_EMOTE_STATS_TABLE = "user_emote_stats";

	const DEFAULT_SERIES_RESOLUTION = 500; // sample points

	public function connect(SilexApplication $app)
	{
		$route = $app['controllers_factory'];
		$db = $app['db'];

		/**
		 * Emote statistics overview
		 */
		$route->get('/', function(Request $request) use($app, $db) {
			// Determine visualized window bounds
			$shownPeriodValue = $request->query->get('shownPeriodValue', 24);
			$shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');
			if ($shownPeriodValue <= 0)
				$shownPeriodValue = 1;

			$windowEndTime = self::getCurrentTimestamp();
			$windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;

			// Get emote statistics
			$emoteStats = [];
			$visualizedEmotes = ['moon2MLEM', 'moon2S', 'moon2A', 'moon2N', 'PogChamp'];
			$minEmoteOccurrences = PHP_INT_MAX;
			foreach ($visualizedEmotes as $emote)
			{
				$stmt = $db->query("SELECT timestamp, total_occurrences FROM ".self::EMOTE_STATS_TABLE." "
								. " WHERE emote='$emote' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime ORDER BY timestamp ASC");
				if ($stmt === false)
					continue;

				if ($stmt->rowCount() > 0)
				{
					$emoteStats[$emote] = self::resampleTimeSeries($stmt->fetchAll(), 'total_occurrences', 100, $windowStartTime, $windowEndTime);
				}
				else
				{
					$emoteStats[$emote] = [
						['timestamp' => $windowStartTime, 'total_occurrences' => 0],
						['timestamp' => $windowEndTime, 'total_occurrences' => 0]
					];
				}

				$firstOccurrences = $emoteStats[$emote][0]['total_occurrences'];
				if ($firstOccurrences < $minEmoteOccurrences)
					$minEmoteOccurrences = $firstOccurrences;
			}

			// Get total message count
			$stmt = $db->query("SELECT timestamp, total_messages FROM channel_stats WHERE timestamp >= $windowStartTime AND timestamp <= $windowEndTime ORDER BY timestamp ASC");
			$channelStats = self::resampleTimeSeries($stmt->fetchAll(), 'total_messages', self::DEFAULT_SERIES_RESOLUTION, $windowStartTime, $windowEndTime);

			// Chatters active in selected time window
			$stmt = $db->query("SELECT channel, username, SUM(messages) AS messages FROM ".self::USER_STATS_TABLE
							. " WHERE timestamp >= $windowStartTime AND timestamp <= $windowEndTime AND messages IS NOT NULL"
							. " GROUP BY channel, username"
							. " ORDER BY SUM(messages) DESC");
			$recentChatters = [];
			$shownRecentChatters = 25;
			$recentChattersMore = $stmt->rowCount();
			for ($i = 0; $i < $shownRecentChatters && ($row = $stmt->fetch()); ++$i)
				$recentChatters[] = $row;

			// Emotes active in selected time window
			$stmt = $db->query("SELECT channel, emote, SUM(occurrences) AS occurrences FROM ".self::EMOTE_STATS_TABLE
							. " WHERE timestamp >= $windowStartTime AND timestamp <= $windowEndTime AND occurrences IS NOT NULL"
							. " GROUP BY channel, emote"
							. " ORDER BY SUM(occurrences) DESC");
			$recentEmotes = [];
			$shownRecentEmotes = 25;
			$recentEmotesMore = $stmt->rowCount();
			for ($i = 0; $i < $shownRecentEmotes && ($row = $stmt->fetch()); ++$i)
				$recentEmotes[] = $row;

			return $app['twig']->render('index.twig', [
				'shownPeriodValue' => $shownPeriodValue,
				'shownPeriodUnit' => $shownPeriodUnit,
				'channelStats' => $channelStats,
				'emoteStats' => $emoteStats,
				'emoteStatsMinOccurrences' => $minEmoteOccurrences,
				'recentChatters' => $recentChatters,
				'recentChattersMore' => $recentChattersMore,
				'recentEmotes' => $recentEmotes,
				'recentEmotesMore' => $recentEmotesMore
			]);
		})->bind('index');

		/**
		 * Emotes leaderboard
		 */
		$route->get('/emotes', function(Request $request) use($app, $db) {
			// Real occurrences (including all chatters)
			$stmt = $db->query("SELECT emotes.emote, type, total_occurrences FROM emotes
								LEFT JOIN (SELECT emote, total_occurrences FROM emote_stats WHERE timestamp = 0) es
									ON es.emote=emotes.emote
								ORDER BY es.total_occurrences DESC");
			if ($stmt === false)
				$app->abort(500, "Query error: " . $db->errorInfo()[2]);

			$emotes = [];
			while ($row = $stmt->fetch())
			{
				$emotes[$row['emote']] = [
					'type' => $row['type'],
					'real_occurrences' => $row['total_occurrences'],
					'occurrences' => 0,
					'standardDeviation' => null
				];
			}

			// Leaderboard without excluded chatters
			// $stmt = $db->query("SELECT emote, MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
			// 				. " WHERE username NOT IN " . self::getExcludedChattersTuple()
			// 				. " GROUP BY emote ORDER BY MAX(total_occurrences) DESC");
			// while ($row = $stmt->fetch())
			// {
			// 	$emote = $row['emote'];
			// 	if (!isset($emotes[$emote]))
			// 		$emotes[$emote] = ['real_occurrences' => 0];

			// 	$emotes[$emote]['occurrences'] = $row['total_occurrences'];

			// 	// Calculate standard deviation of emote usages by users
			// 	/*$stmt2 = $db->query("SELECT MAX(total_occurrences) FROM ".self::USER_EMOTE_STATS_TABLE." WHERE emote='$emote' AND username != '_streamelements_' GROUP BY username ORDER BY MAX(total_occurrences)");
			// 	$occurrences = array_map(function($row) { return $row[0]; }, $stmt2->fetchAll());
			// 	$emotes[$emote]['standardDeviation'] = self::getDeviationFrom($occurrences, $emotes[$emote]['real_occurrences']);*/
			// 	$emotes[$emote]['standardDeviation'] = 0;
			// }

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

			// Assign ranks
			$rank = 1;
			foreach ($emotes as $emoteName => $emoteStats)
				$emotes[$emoteName]['rank'] = $rank++;

			return $app['twig']->render('emotes.twig', [
				'emotes' => $emotes]);
		})->bind('emotes');

		/**
		 * User Leaderboard for an emote
		 */
		$route->get('/emote/{emote}', function(Request $request, $emote) use($app, $db) {
			// Determine visualized window bounds
			$shownPeriodValue = $request->query->get('shownPeriodValue', 0);
			$shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');
			
			$windowEndTime = self::getCurrentTimestamp();
			if ($shownPeriodValue <= 0)
				$windowStartTime = 1;
			else
				$windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;
			
			// Get emote metadata
			$stmt = $db->query("SELECT emote, type FROM ".self::EMOTES_TABLE." WHERE emote='$emote' LIMIT 1");
			if ($stmt->rowCount() == 0)
				$app->abort(404, "Emote not found");

			$emoteType = $stmt->fetch()['type'];
			
			// Get emote stats
			$stmt = $db->query("SELECT timestamp, total_occurrences FROM ".self::EMOTE_STATS_TABLE
							. " WHERE emote='$emote' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. " ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();
			$minOccurrences = $stats[0]['total_occurrences'];
			$stats = self::resampleTimeSeries($stats, 'total_occurrences', self::DEFAULT_SERIES_RESOLUTION);

			// Get total occurrences
			$stmt = $db->query("SELECT SUM(total_occurrences) FROM ("
							. "   SELECT MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. "   WHERE emote='$emote' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. "   GROUP BY username) a");
			$totalOccurences = $stmt->fetch()[0];

			// Leaderboard
			$stmt = $db->query("SELECT username, MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. " WHERE emote='$emote' AND username NOT IN " . self::getExcludedChattersTuple() . " AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. " GROUP BY username ORDER BY MAX(total_occurrences) DESC LIMIT 1000");
			$leaderboard = [];
			while ($row = $stmt->fetch())
			{
				$row['percentage'] = 100.0 * ($row['total_occurrences'] / $totalOccurences);
				$leaderboard[] = $row;
			}

			// Trim window range to actual data
			$windowStartTime = $stats[0]['timestamp'];
			$windowEndTime = $stats[max(0, count($stats) -1)]['timestamp'];

			return $app['twig']->render('emote.twig', [
				'emote' => $emote,
				'emoteType' => $emoteType,
				'shownPeriodValue' => $shownPeriodValue,
				'shownPeriodUnit' => $shownPeriodUnit,
				'windowStart' => $windowStartTime,
				'windowEnd' => $windowEndTime,
				'leaderboard' => $leaderboard,
				'stats' => $stats,
				'totalOccurrences' => $totalOccurences,
				'totalOccurrences2' => $stats[count($stats) - 1]['total_occurrences'],
				'minOccurrences' => $minOccurrences]);
		})->bind('emote');

		/**
		 * Per-user stats for an emote
		 */
		$route->get('/emote/{emote}/user/{username}', function(Request $request, $emote, $username) use($app, $db) {
			// Determine visualized window bounds
			$shownPeriodValue = $request->query->get('shownPeriodValue', 0);
			$shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');
			
			$windowEndTime = self::getCurrentTimestamp();
			if ($shownPeriodValue <= 0)
				$windowStartTime = 1;
			else
				$windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;
			
			// Get stats
			$stmt = $db->query("SELECT timestamp, total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE
							. " WHERE emote='$emote' AND username='$username' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. " ORDER BY timestamp ASC");
			$stats = self::resampleTimeSeries($stmt->fetchAll(), 'total_occurrences', self::DEFAULT_SERIES_RESOLUTION);
			
			// Trim window range into data range
			$windowStartTime = $stats[0]['timestamp'];
			$windowEndTime = $stats[max(0, count($stats) - 1)]['timestamp'];

			return $app['twig']->render('user_emote.twig', [
				'emote' => $emote,
				'shownPeriodValue' => $shownPeriodValue,
				'shownPeriodUnit' => $shownPeriodUnit,
				'windowStart' => $windowStartTime,
				'windowEnd' => $windowEndTime,
				'username' => $username,
				'stats' => $stats]);
		})->bind('emote_user');

		/**
		 * Users leaderboard on total message count
		 */
		$route->get('/users', function(Request $request) use($app, $db) {
			$excluded_users = ["nightbot"];
			$max_rank = $request->query->get('max', 100);
			
			$stmt = $db->query("SELECT username, total_messages FROM ".self::USER_STATS_TABLE." WHERE timestamp = 0 ORDER BY total_messages DESC LIMIT ".($max_rank + count(self::EXCLUDED_CHATTERS)));
			if ($stmt === false)
				$app->abort(500, "Query error: " . $db->errorInfo()[2]);
			
			$leaderboard = [];
			$rank = 1;
			while ($row = $stmt->fetch())
			{
				$username = $row['username'];
				$is_bot = in_array($username, self::EXCLUDED_CHATTERS);
				$row['is_bot'] = $is_bot;
				$row['rank'] = $is_bot ? '' : $rank++;
				$leaderboard[] = $row;
			}

			return $app['twig']->render('users.twig', [
				'users' => $leaderboard,
				'shownRanks' => $max_rank
			]);
		})->bind('users');

		/**
		 * User message count
		 */
		$route->get('/user/{username}', function(Request $request, $username) use($app, $db) {
			// Determine visualized window bounds
			$shownPeriodValue = $request->query->get('shownPeriodValue', 0);
			$shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');
			
			$windowEndTime = self::getCurrentTimestamp();
			if ($shownPeriodValue <= 0)
				$windowStartTime = 1;
			else
				$windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;
			
			// User message count stats
			$stmt = $db->query("SELECT timestamp, total_messages FROM ".self::USER_STATS_TABLE.""
							. " WHERE username='$username' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. " ORDER BY timestamp ASC");
			if ($stmt === false)
				$app->abort(500, "Query error: " . $db->errorInfo()[2]);

			if ($stmt->rowCount() == 0)
				$app->abort(404, "No records found for this user in the given time range.");

			$stats = self::resampleTimeSeries($stmt->fetchAll(), 'total_messages', self::DEFAULT_SERIES_RESOLUTION);
			if ($windowStartTime == 0)
				$windowStartTime = $stats[0]['timestamp'];

			// Emote leaderboard for this user
			$stmt = $db->query("SELECT emote, MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
							. " WHERE username='$username' AND timestamp >= $windowStartTime AND timestamp <= $windowEndTime"
							. " GROUP BY emote ORDER BY MAX(total_occurrences) DESC");
			$emoteUsages = [];
			while ($row = $stmt->fetch())
				$emoteUsages[$row['emote']] = $row['total_occurrences'];
			
			// Trim window range to actual data
			$windowStartTime = $stats[0]['timestamp'];
			$windowEndTime = $stats[max(0, count($stats) - 1)]['timestamp'];

			return $app['twig']->render('user.twig', [
				'username' => $username,
				'shownPeriodValue' => $shownPeriodValue,
				'shownPeriodUnit' => $shownPeriodUnit,
				'windowStart' => $windowStartTime,
				'windowEnd' => $windowEndTime,
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

	static function convertTimePeriod($fromValue, $fromUnit, $toUnit)
	{
		return $fromValue * self::getTimeUnitSecondsMultiplier($fromUnit)
			/ self::getTimeUnitSecondsMultiplier($toUnit);
	}

	// Returns number of seconds in the given time unit, or 1 if not known
	// Note that months are assumed to be all 30 days long and a year be 365 days long.
	static function getTimeUnitSecondsMultiplier($unit)
	{
		$units = [
			'minutes' => 60,
			'hours' => 60 * 60,
			'days' => 24 * 60 * 60,
			'weeks' => 7 * 24 * 60 * 60,
			'months' => 30 * 24 * 60 * 60,
			'years' => 365 * 24 * 60 * 60,
		];
		return array_key_exists($unit, $units) ? $units[$unit] : 1;
	}

	// Re-Samples the given time series to reduce or increase resolution in time domain
	// This function assumes that series is already sorted by timestamp and is in the following format:
	//	[['timestamp' => ..., $fieldName => ''], ...]
	// numPoints is the number of points of the output series
	// startTime and endTime are only used if the series is empty and should be set to the visible view window
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

		// Do not up-sample the timeseries
		if ($numPoints >= $n)
			return $series;

		$first = reset($series);
		$last = end($series);

		$startTime = $first['timestamp'];
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
