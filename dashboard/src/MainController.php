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

	public function connect(SilexApplication $app)
	{
		$route = $app['controllers_factory'];
		$db = $app['db'];

		/**
		 * Emote statistics overview
		 */
		$route->get('/', function(Request $request) use($app, $db) {
			// Get emote list
			$stmt = $db->query("SELECT DISTINCT a.emote AS emote, LOWER(a.emote) AS ignored, b.type AS type"
							. " FROM ".self::EMOTE_STATS_TABLE." a"
							. " LEFT JOIN ".self::EMOTES_TABLE." b ON a.emote=b.emote"
							. " ORDER BY LOWER(a.emote) ASC");
			if ($stmt === false)
				print_r($db->errorInfo());
			$emotes = [];
			while ($row = $stmt->fetch())
				$emotes[$row['emote']] = $row['type'];

			// Determine visualized window bounds
			$shownMinutes = $request->query->get('shownMinutes', 24 * 60);
			$latestTimestamp = round(microtime(true) * 1000);
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
					$emoteStats[$emote] = $stmt->fetchAll();
					$firstOccurrences = $emoteStats[$emote][0]['occurrences'];
				}
				else
				{
					$emoteStats[$emote] = [
						['timestamp' => $earliestTimestamp, 'occurrences' => 0],
						['timestamp' => $latestTimestamp, 'occurrences' => 0]
					];
				}

				if ($firstOccurrences < $minEmoteOccurrences)
					$minEmoteOccurrences = $firstOccurrences;
			}

			// Get total message count
			$stmt = $db->query("SELECT timestamp, message_count FROM channel_stats WHERE timestamp >= $earliestTimestamp AND timestamp <= $latestTimestamp ORDER BY timestamp ASC");
			$channelStats = $stmt->fetchAll();

			// Get top N users stats in selected time window
			$topN = 10;
			$additional_chatters = [];
			$stmt = $db->query("SELECT username FROM ".self::USER_STATS_TABLE." WHERE timestamp >= $earliestTimestamp GROUP BY username ORDER BY MAX(message_count) DESC LIMIT " . ($topN + count(self::EXCLUDED_CHATTERS)));
			$topChatterUsernames = [];
			while ($row = $stmt->fetch())
				$topChatterUsernames[] = $row[0];

			foreach ($additional_chatters as $username)
			{
				if (!in_array($username, $topChatterUsernames))
					$topChatterUsernames[] = $username;
			}

			$topChatters = [];
			foreach ($topChatterUsernames as $username)
			{
				if (in_array($username, self::EXCLUDED_CHATTERS))
					continue;

				$stmt2 = $db->query("SELECT timestamp, message_count FROM ".self::USER_STATS_TABLE." WHERE username='$username' AND timestamp >= $earliestTimestamp ORDER BY timestamp ASC");
				if ($stmt2->rowCount() > 0)
					$topChatters[$username] = $stmt2->fetchAll();
				
				if (count($topChatters) == $topN)
					break;
			}

			return $app['twig']->render('index.twig', [
				'emotes' => $emotes,
				'shownMinutes' => $shownMinutes,
				'channelStats' => $channelStats,
				'emoteStats' => $emoteStats,
				'emoteStatsMinOccurrences' => $minEmoteOccurrences,
				'topChatters' => $topChatters]);
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
			$stmt = $db->query("SELECT timestamp, occurrences FROM ".self::EMOTE_STATS_TABLE." WHERE emote='$emote' ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();
			$minOccurrences = $stats[0]['occurrences'];

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
			$stats = $stmt->fetchAll();
			
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
			
			$stmt = $db->query("SELECT username, MAX(message_count) AS message_count FROM ".self::USER_STATS_TABLE." GROUP BY username ORDER BY MAX(message_count) DESC");
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
			$stmt = $db->query("SELECT timestamp, message_count FROM ".self::USER_STATS_TABLE." WHERE username='$username' ORDER BY timestamp ASC");
			if ($stmt->rowCount() == 0)
				$app->abort(404, "User not found");

			$stats = $stmt->fetchAll();

			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM ".self::USER_EMOTE_STATS_TABLE." WHERE username='$username' GROUP BY emote ORDER BY MAX(occurrences) DESC");
			$emoteUsages = [];
			while ($row = $stmt->fetch())
				$emoteUsages[$row['emote']] = $row['occurrences'];
			
			return $app['twig']->render('user.twig', [
				'username' => $username,
				'stats' => $stats,
				'emoteUsages' => $emoteUsages]);
		})->bind('user');

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
}
