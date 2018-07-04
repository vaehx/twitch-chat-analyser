<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;

class MainController implements ControllerProviderInterface
{
	const EXCLUDED_CHATTERS = ["nightbot", "notheowner", "_streamelements_", "scootycoolguy"];

	public function connect(SilexApplication $app)
	{
		$route = $app['controllers_factory'];
		$db = $app['db'];

		/**
		 * Emote statistics overview
		 */
		$route->get('/', function(Request $request) use($app, $db) {
			$stmt = $db->query("SELECT DISTINCT emote FROM emotes ORDER BY emote ASC");
			$emotes = [];
			while ($row = $stmt->fetch())
				$emotes[] = $row[0];

			$stmt = $db->query("SELECT MAX(timestamp) FROM emotes");
			$latestTimestamp = $stmt->fetch()[0];
			
			$shownMinutes = $request->query->get('shownMinutes', 24 * 60);
			$earliestTimestamp = $latestTimestamp - $shownMinutes * 60 * 1000;

			// Get emote statistics
			$emoteStats = [];
			$visualizedEmotes = ['moon2MLEM', 'moon2S', 'moon2A', 'moon2N', 'PogChamp'];
			$minEmoteOccurrences = PHP_INT_MAX;
			foreach ($visualizedEmotes as $emote)
			{
				$stmt = $db->query("SELECT timestamp, occurrences FROM emote_totals "
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

			// Get top N users stats in selected time window
			$topN = 10;
			$additional_chatters = [];
			$stmt = $db->query("SELECT username FROM users WHERE timestamp >= $earliestTimestamp GROUP BY username ORDER BY MAX(message_count) DESC LIMIT " . ($topN + count(self::EXCLUDED_CHATTERS)));
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

				$stmt2 = $db->query("SELECT timestamp, message_count FROM users WHERE username='$username' AND timestamp >= $earliestTimestamp ORDER BY timestamp ASC");
				if ($stmt2->rowCount() > 0)
					$topChatters[$username] = $stmt2->fetchAll();
				
				if (count($topChatters) == $topN)
					break;
			}

			return $app['twig']->render('index.twig', [
				'emotes' => $emotes,
				'shownMinutes' => $shownMinutes,
				'emoteStats' => $emoteStats,
				'emoteStatsMinOccurrences' => $minEmoteOccurrences,
				'topChatters' => $topChatters]);
		})->bind('index');

		/**
		 * Emotes leaderboard
		 */
		$route->get('/emotes', function(Request $request) use($app, $db) {
			// Real occurrences (including all chatters)
			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM emote_totals"
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
			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM emotes"
							. " WHERE username NOT IN " . self::getExcludedChattersTuple()
							. " GROUP BY emote ORDER BY MAX(occurrences) DESC");
			while ($row = $stmt->fetch())
			{
				$emote = $row['emote'];
				if (!isset($emotes[$emote]))
					$emotes[$emote] = ['real_occurrences' => 0];

				$emotes[$emote]['occurrences'] = $row['occurrences'];

				// Calculate standard deviation of emote usages by users
				$stmt2 = $db->query("SELECT MAX(occurrences) FROM emotes WHERE emote='$emote' AND username != '_streamelements_' GROUP BY username ORDER BY MAX(occurrences)");
				$occurrences = array_map(function($row) { return $row[0]; }, $stmt2->fetchAll());
				$emotes[$emote]['standardDeviation'] = self::standardDeviation($occurrences);
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
			$stmt = $db->query("SELECT timestamp, occurrences FROM emote_totals WHERE emote='$emote' ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();
			$minOccurrences = $stats[0]['occurrences'];

			// Get total occurrences
			$stmt = $db->query("SELECT SUM(occurrences) FROM ("
							. "   SELECT MAX(occurrences) AS occurrences FROM emotes"
							. "   WHERE emote='$emote' AND username != '_streamelements_' GROUP BY username) a");
			$totalOccurences = $stmt->fetch()[0];

			// Leaderboard
			$stmt = $db->query("SELECT username, MAX(occurrences) AS occurrences FROM emotes"
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
			$stmt = $db->query("SELECT timestamp, occurrences FROM emotes WHERE emote='$emote' AND username='$username' ORDER BY timestamp ASC");
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
			
			$stmt = $db->query("SELECT username, MAX(message_count) AS message_count FROM users GROUP BY username ORDER BY MAX(message_count) DESC");
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
			$stmt = $db->query("SELECT timestamp, message_count FROM users WHERE username='$username' ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();

			$stmt = $db->query("SELECT emote, MAX(occurrences) AS occurrences FROM emotes WHERE username='$username' GROUP BY emote ORDER BY MAX(occurrences) DESC");
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
			. "   FROM (SELECT DISTINCT timestamp FROM emotes WHERE emote='$emote' AND timestamp >= $earliestTimestamp) t"
			. "   INNER JOIN (SELECT * FROM emotes WHERE emote='$emote' AND username NOT IN " . self::getExcludedChattersTuple() . ") e"
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

	static function standardDeviation($array)
	{
		// square root of sum of squares devided by N-1
		$n = count($array);
		if ($n <= 1)
			return null;

		$sum_array = array_fill(0, $n, (array_sum($array) / $n));
		return sqrt(array_sum(array_map(function($x, $mean) { return pow($x - $mean, 2); }, $array, $sum_array)) / ($n - 1));
	}
}
