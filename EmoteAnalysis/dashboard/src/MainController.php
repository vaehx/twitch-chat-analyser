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

		$route->get('/', function() use($app, $db) {
			$stmt = $db->query("SELECT DISTINCT emote FROM emotes");
			$emotes = [];
			while ($row = $stmt->fetch())
				$emotes[] = $row[0];

			$mlemStats = self::getEmoteStatistics('moon2MLEM', $db);
			$smileStats = self::getEmoteStatistics('moon2S', $db);
			$angryStats = self::getEmoteStatistics('moon2A', $db);
			$neutStats = self::getEmoteStatistics('moon2N', $db);
			$pogChampStats = self::getEmoteStatistics('PogChamp', $db);

			return $app['twig']->render('index.twig', [
				'emotes' => $emotes,
				'mlemStats' => $mlemStats,
				'smileStats' => $smileStats,
				'angryStats' => $angryStats,
				'neutStats' => $neutStats,
				'pogChampStats' => $pogChampStats]);
		})->bind('index');

		$route->get('/emote/{emote}', function($emote) use($app, $db) {
			$stmt = $db->query("SELECT * FROM emotes WHERE emote='$emote'");
			$stats = $stmt->fetchAll();

			$stmt = $db->query("SELECT DISTINCT username FROM emotes WHERE emote='$emote'");
			$users = [];
			while ($row = $stmt->fetch())
				$users[] = $row[0];
			sort($users);

			return $app['twig']->render('emote.twig', [
				'emote' => $emote,
				'stats' => $stats,
				'users' => $users]);
		})->bind('emote');

		return $route;
	}

	// Returns array of (timestamp, occurrences) tuples sorted by timestamp for this emote
	static function getEmoteStatistics($emote, $db) {
		$stmt = $db->query("SELECT DISTINCT timestamp FROM emotes WHERE emote='$emote' ORDER BY timestamp ASC");
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
