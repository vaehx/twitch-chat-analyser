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

        $route->get('/', function() use($app) {
            return "";
        })->bind('api_index');

        /**
         * Emote statistics
         */
        $route->get('/emote_stats', function(Request $request) use($app, $db) {
            $sql = "SELECT channel, emote, occurrences FROM " . self::EMOTE_STATS_TABLE . " WHERE timestamp = 0";
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
                        'total_occurrences' => $stat['occurrences']
                    );
                }
            }

            return $app->json($result);
        })->bind('api_channel_emote_stats');

        /**
         * Statistics about a specific user
         */
        $route->get('/user/{username}', function($username) use($app, $db) {
            $result = ['channels' => []];

            // Get total message count in each channel
            $stmt = $db->prepare("SELECT channel, messages FROM user_stats WHERE username = :username AND timestamp = 0");
            $res = $stmt->execute(array(':username' => $username));

            if ($res === false || $stmt->rowCount() == 0)
                return $app->json($result);

            while ($row = $stmt->fetch())
                $result['channels'][$row['channel']] = ['messages' => $row['messages']];

            // Get last seen time
            $stmt = $db->prepare("SELECT channel, MAX(timestamp) AS last_seen FROM user_stats WHERE username = :username GROUP BY channel");
            $res = $stmt->execute(array(':username' => $username));

            if ($res === false || $stmt->rowCount() == 0)
                return $app->json($result);

            while ($row = $stmt->fetch())
                $result['channels'][$row['channel']]['last_seen'] = floor($row['last_seen'] / 1000); // ms -> seconds

            return $app->json($result);
        })->bind('api_user');

        /**
         * Statistics about an emote used by a specific user
         */
        $route->get('/user/{username}/emote/{emote}', function($username, $emote) use($app, $db) {
            $result = ['channels' => []];

            $stmt = $db->prepare("SELECT c.channel, s.occurrences FROM ("
                ."SELECT DISTINCT channel FROM channel_stats) AS c "
                ."LEFT JOIN (SELECT channel, occurrences FROM user_emote_stats WHERE emote = :emote AND username = :username AND timestamp = 0) AS s "
                ."ON c.channel = s.channel");
            $res = $stmt->execute(array(':username' => $username, ':emote' => $emote));

            if ($res === false)
                $app->abort(500, "Internal query error");

            while ($row = $stmt->fetch())
                $result['channels'][$row['channel']] = ['occurrences' => $row['occurrences'] !== null ? $row['occurrences'] : 0];

            return $app->json($result);
        })->bind('api_user_emote');

        /**
         * Statistics about all channels
         */
        $route->get('/channels', function() use($app, $db) {
            $result = [];
            $stmt = $db->prepare("SELECT channel, messages FROM channel_stats WHERE timestamp = 0");
            $res = $stmt->execute([]);

            if ($res === false)
                $app->abort(500, "Internal query error");

            while ($row = $stmt->fetch())
                $result[$row['channel']] = $row['messages'];

            return $app->json($result);
        })->bind('api_channels');

        /**
         * Statistics about a channel
         */
        $route->get('/channel/{channel}', function($channel) use($app, $db) {
            $result = [];
            
            // Get latest message counts
            $stmt = $db->prepare("SELECT messages FROM channel_stats WHERE channel = :channel AND timestamp = 0");
            $res = $stmt->execute(array(':channel' => $channel));

            if ($res === false || $stmt->rowCount() == 0)
                $app->abort(404, 'Channel not found');

            $row = $stmt->fetch();
            $result['total_messages'] = $row['messages'];

            $times = [
                'messages_last_5min' => 5 * 60 * 1000,
                'messages_last_1h' => 60 * 60 * 1000,
                'messages_last_24h' => 24 * 60 * 60 * 1000,
                'messages_last_7d' => 7 * 24 * 60 * 60 * 1000,
                'messages_last_month' => 30 * 24 * 60 * 60 * 1000
            ];

            date_default_timezone_set('Europe/Berlin');
            $now = time() * 1000;
            $stmt = $db->prepare('SELECT SUM(messages) AS messages FROM channel_stats WHERE channel = :channel AND timestamp > :t');
            foreach ($times as $name => $dt)
            {
                $res = $stmt->execute(array(':channel' => $channel, ':t' => $now - $dt));
                if ($res !== false && $stmt->rowCount() > 0)
                {
                    $row = $stmt->fetch();
                    $result[$name] = $row['messages'] !== null ? $row['messages'] : 0;
                }
                else
                {
                    $result[$name] = 0;
                }
            }

            return $app->json($result);
        })->bind('api_channel');

        return $route;
    }
}
