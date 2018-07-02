<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;

class MainController implements ControllerProviderInterface
{
	public function connect(SilexApplication $app)
	{
		$route = $app['controllers_factory'];
		$db = $app['db'];

		/**
		 * Emote statistics overview
		 */
		$route->get('/', function(Request $request) use($app, $db) {
			$stmt = $db->query("SELECT DISTINCT emote FROM emotes");
			$emotes = [];
			while ($row = $stmt->fetch())
				$emotes[] = $row[0];

			$stmt = $db->query("SELECT MAX(timestamp) FROM emotes");
			$latestTimestamp = $stmt->fetch()[0];
			
			$shownMinutes = $request->query->get('shownMinutes', 5 * 60);
			$earliestTimestamp = $latestTimestamp - $shownMinutes * 60 * 1000;

			$mlemStats = self::getEmoteStatistics('moon2MLEM', $db, $earliestTimestamp);
			$smileStats = self::getEmoteStatistics('moon2S', $db, $earliestTimestamp);
			$angryStats = self::getEmoteStatistics('moon2A', $db, $earliestTimestamp);
			$neutStats = self::getEmoteStatistics('moon2N', $db, $earliestTimestamp);
			$pogChampStats = self::getEmoteStatistics('PogChamp', $db, $earliestTimestamp);

			return $app['twig']->render('index.twig', [
				'emotes' => $emotes,
				'shownMinutes' => $shownMinutes,
				'mlemStats' => $mlemStats,
				'smileStats' => $smileStats,
				'angryStats' => $angryStats,
				'neutStats' => $neutStats,
				'pogChampStats' => $pogChampStats]);
		})->bind('index');

		/**
		 * Emote Leaderboard
		 */
		$route->get('/emote/{emote}', function($emote) use($app, $db) {
			$stmt = $db->query("SELECT username, MAX(occurrences) AS occurrences FROM emotes WHERE emote='$emote' GROUP BY username ORDER BY MAX(occurrences) DESC");
			$leaderboard = $stmt->fetchAll();

			return $app['twig']->render('emote.twig', [
				'emote' => $emote,
				'leaderboard' => $leaderboard]);
		})->bind('emote');

		/**
		 * Per-user stats for an emote
		 */
		$route->get('/emote/{emote}/user/{username}', function($emote, $username) use($app, $db) {
			$stmt = $db->query("SELECT timestamp, occurrences FROM emotes WHERE emote='$emote' AND username='$username' ORDER BY timestamp ASC");
			$stats = $stmt->fetchAll();
			
			return $app['twig']->render('user.twig', [
				'emote' => $emote,
				'username' => $username,
				'stats' => $stats]);
		})->bind('emote_user');

		return $route;
	}

	// Returns array of (timestamp, occurrences) tuples sorted by timestamp for this emote
	static function getEmoteStatistics($emote, $db, $earliestTimestamp=0) {
		$stmt = $db->query("SELECT DISTINCT timestamp FROM emotes WHERE emote='$emote' AND timestamp >= $earliestTimestamp ORDER BY timestamp ASC");
		$stats = [];
		while ($row = $stmt->fetch())
		{
			$ts = $row[0];
			$stmt2 = $db->query("SELECT SUM(maxocc) AS occurrences FROM " .
				"(SELECT MAX(occurrences) AS maxocc FROM emotes WHERE emote='$emote' AND timestamp <= $ts GROUP BY username) t");
			$stats[] = [
				'timestamp' => $ts,
				'occurrences' => $stmt2->fetch()[0]
			];
		}

		return $stats;
	}
}
