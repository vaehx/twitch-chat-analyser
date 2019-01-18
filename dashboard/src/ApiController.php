<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;

class ApiController implements ControllerProviderInterface
{
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
		 * Emote statistics
		 */
		$route->get('/emote_stats', function(Request $request) use($app, $db) {
			$sql = "SELECT channel, emote, total_occurrences FROM " . self::EMOTE_STATS_TABLE . " WHERE timestamp = 0";
			$params = array();
			if ($request->query->has('emotes'))
			{
				$emotes = explode(',', $request->query->get('emotes'));
				$emotes = array_map('trim', $emotes);

				$placeholders = implode(',', array_fill(0, count($emotes), '?'));
				$sql .= " AND emote IN ($placeholders)";

				$params = array_merge($params, $emotes);
			}

			$stmt = $db->prepare($sql);
			$stmt->execute($params);
			$stats = $stmt->fetchAll();

			$result = ['channels' => []];
			if (!empty($stats))
			{
				foreach ($stats as $stat)
				{
					$channel = $stat['channel'];
					if (!array_key_exists($channel, $result['channels']))
						$result['channels'][$channel] = [];

					$result['channels'][$channel][$stat['emote']] = (object)array(
						'total_occurrences' => $stat['total_occurrences']
					);
				}
			}

			return $app->json($result);
		})->bind('api_channel_emote_stats');

		return $route;
	}
}
