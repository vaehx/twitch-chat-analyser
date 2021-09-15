<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Api\ControllerProviderInterface;
use Symfony\Component\HttpFoundation\Request;
use \DateTime;

class MainController implements ControllerProviderInterface
{
    const EXCLUDED_CHATTERS = ["nightbot", "notheowner", "scootycoolguy"];

    const CHANNELS_TABLE = "channels";
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
         * Channel overview
         */
        $route->get('/', function(Request $request) use($app, $db) {
            if ($request->query->has('windowStartText') && $request->query->has('windowEndText')) {
                $windowStartTime = self::htmlInputTextToTimestamp($request->query->get('windowStartText'));
                $windowEndTime = self::htmlInputTextToTimestamp($request->query->get('windowEndText'));
            } else {
                $windowEndTime = self::getCurrentTimestamp();
                $windowStartTime = $windowEndTime - self::convertTimePeriod(1, 'weeks', 'millis');
            }

            // Get channel meta
            $stmt = $db->query("SELECT DISTINCT channel AS name, messages FROM ".self::CHANNEL_STATS_TABLE." WHERE timestamp = 0"
                    ." AND ".self::getHiddenChannelsCondition());
            if ($stmt === false)
                $app->abort(500, "Query error: " . $db->errorInfo()[2]);
            $channels = $stmt->fetchAll();

            // Get message graph data for each channel
            foreach ($channels as $k => &$channel) {
                // Get window start total count
                $stmt = $db->prepare("SELECT COALESCE(sum(messages), 0) AS window_start_total_messages FROM ".self::CHANNEL_STATS_TABLE
                                . " WHERE channel = :channel AND timestamp > 0 AND timestamp < :windowStartTime");
                $res = $stmt->execute(array(
                    ':channel' => $channel['name'],
                    ':windowStartTime' => $windowStartTime));
                if ($res === false)
                    $app->abort(500, "Could not query for window start total count for channel '".$channel['name'].": " . self::getStmtErrorMessage($stmt));

                $channel['windowStartTotalMessages'] = $stmt->fetch()['window_start_total_messages'];

                // Get graph data within window
                $stmt = $db->prepare("SELECT timestamp, messages FROM ".self::CHANNEL_STATS_TABLE
                                . " WHERE channel = :channel AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                                . " ORDER BY timestamp ASC");
                $res = $stmt->execute(array(
                    ':channel' => $channel['name'],
                    ':windowStartTime' => $windowStartTime,
                    ':windowEndTime' => $windowEndTime));
                if ($res === false)
                    $app->abort(500, "Could not query for graph data for channel '".$channel['name']."': " . self::getDBErrorMessage($db));

                $samples = self::checkEmptyTimeseries($stmt->fetchAll(), 'messages', $windowStartTime, $windowEndTime);
                $samples = self::ratesToCumulativeSums($samples, 'messages', $channel['windowStartTotalMessages']);
                $samples = self::resampleTimeSeries($samples, 'messages', 100, $windowStartTime, $windowEndTime);

                $channel['totalMessageCountSamples'] = $samples;
            }
            unset($channel);

            return $app['twig']->render('index.twig', [
                'channels' => $channels,
                'windowStartTime' => $windowStartTime,
                'windowEndTime' => $windowEndTime,
                'windowStartText' => self::timestampToHTMLInputText($windowStartTime),
                'windowEndText' => self::timestampToHTMLInputText($windowEndTime),
            ]);
        })->bind('index');

        /**
         * Emote statistics overview for a channel
         */
        $route->get('/channel/{channel}', function(Request $request, $channel) use($app, $db) {
            $timer = new Timer();
            $timer->start();

            if ($request->query->has('windowStartText') && $request->query->has('windowEndText')) {
                $windowStartTime = self::htmlInputTextToTimestamp($request->query->get('windowStartText'));
                $windowEndTime = self::htmlInputTextToTimestamp($request->query->get('windowEndText'));
            } else {
                $windowEndTime = self::getCurrentTimestamp();
                $windowStartTime = $windowEndTime - self::convertTimePeriod(1, 'weeks', 'millis');
            }

            // Get channel info
            $stmt = $db->prepare("SELECT * FROM channel_stats WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND timestamp = 0");
            $stmt->execute(array(':channel' => $channel));
            if ($stmt->rowCount() == 0)
                $app->abort(404, "No data found for that channel");
            $channelInfo = $stmt->fetch();
            $timer->mark('get_channel_info');

            // Get emote statistics
            $emoteStats = [];
            $emoteStatsWindowStartOccurrences = [];
            $visualizedEmotes = ['moon2MLEM', 'moon2S', 'moon2A', 'moon2N', 'PogChamp'];
            $minEmoteOccurrences = PHP_INT_MAX;
            foreach ($visualizedEmotes as $emote) {
                // Get window start total count
                $stmt = $db->prepare("SELECT COALESCE(sum(occurrences), 0) AS window_start_total_occurrences FROM ".self::EMOTE_STATS_TABLE
                                . " WHERE channel = :channel AND emote = :emote AND timestamp > 0 AND timestamp < :windowStartTime");
                $res = $stmt->execute(array(
                    ':channel' => $channel,
                    ':emote' => $emote,
                    ':windowStartTime' => $windowStartTime));

                $emoteStatsWindowStartOccurrences[$emote] = $stmt->fetch()['window_start_total_occurrences'];

                // Get samples
                $stmt = $db->prepare("SELECT timestamp, occurrences FROM ".self::EMOTE_STATS_TABLE." "
                                . " WHERE channel = :channel AND emote = :emote AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                                . " ORDER BY timestamp ASC");
                $res = $stmt->execute(array(
                    ':channel' => $channel,
                    ':emote' => $emote,
                    ':windowStartTime' => $windowStartTime,
                    ':windowEndTime' => $windowEndTime));

                $samples = self::checkEmptyTimeseries($stmt->fetchAll(), 'occurrences', $windowStartTime, $windowEndTime);
                $samples = self::ratesToCumulativeSums($samples, 'occurrences', $emoteStatsWindowStartOccurrences[$emote]);
                $samples = self::resampleTimeSeries($samples, 'occurrences', 100, $windowStartTime, $windowEndTime);

                $emoteStats[$emote] = $samples;
            }
            $timer->mark('get_emote_statistics');

            // Get total message count in this channel
            $stmt = $db->prepare("SELECT COALESCE(sum(messages), 0) AS window_start_total_messages FROM ".self::CHANNEL_STATS_TABLE
                            . " WHERE channel = :channel AND timestamp > 0 AND timestamp < :windowStartTime");
            $res = $stmt->execute(array(
                ':channel' => $channel,
                ':windowStartTime' => $windowStartTime));
            if ($res === false)
                $app->abort(500, "Could not query window start channel total messages: " . self::getStmtErrorMessage($stmt));

            $channelWindowStartTotalMessages = $stmt->fetch()['window_start_total_messages'];

            $stmt = $db->prepare("SELECT timestamp, messages FROM channel_stats"
                            . " WHERE channel = :channel AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " ORDER BY timestamp ASC");
            $res = $stmt->execute(array(
                ':channel' => $channel,
                ':windowStartTime' => $windowStartTime,
                ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query channel messages graph timeseries: " . self::getStmtErrorMessage($stmt));

            $samples = self::checkEmptyTimeseries($stmt->fetchAll(), 'messages', $windowStartTime, $windowEndTime);
            $samples = self::ratesToCumulativeSums($samples, 'messages', $channelWindowStartTotalMessages);
            $samples = self::resampleTimeSeries($samples, 'messages', self::DEFAULT_SERIES_RESOLUTION, $windowStartTime, $windowEndTime);

            $channelStats = $samples;

            $timer->mark('get_total_message_count');

            // Chatters active in selected time window
            $stmt = $db->prepare("SELECT channel, username, SUM(messages) AS messages FROM ".self::USER_STATS_TABLE
                            . " WHERE channel = :channel AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime AND messages IS NOT NULL"
                            . " GROUP BY channel, username"
                            . " ORDER BY SUM(messages) DESC");
            $res = $stmt->execute(array(
                ':channel' => $channel,
                ':windowStartTime' => $windowStartTime,
                ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query active chatters in time window: " . self::getStmtErrorMessage($stmt));

            $recentChatters = [];
            $shownRecentChatters = 25;
            $recentChattersMore = $stmt->rowCount();
            for ($i = 0; $i < $shownRecentChatters && ($row = $stmt->fetch()); ++$i)
                $recentChatters[] = $row;
            $timer->mark('get_recent_chatters');

            // Emotes active in selected time window
            $stmt = $db->prepare("SELECT channel, emote, SUM(occurrences) AS occurrences FROM ".self::EMOTE_STATS_TABLE
                            . " WHERE channel = :channel AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime AND occurrences IS NOT NULL"
                            . " GROUP BY channel, emote"
                            . " ORDER BY SUM(occurrences) DESC");

            $res = $stmt->execute(array(':channel' => $channel, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query used emotes in time window: " . self::getDBErrorMessage($db));

            $recentEmotes = [];
            $shownRecentEmotes = 25;
            $recentEmotesMore = $stmt->rowCount();
            for ($i = 0; $i < $shownRecentEmotes && ($row = $stmt->fetch()); ++$i)
                $recentEmotes[] = $row;
            $timer->mark('get_recent_emotes');

            return $app['twig']->render('channel.twig', [
                'channel' => $channel,
                'timer' => $timer,
                'windowStartTime' => $windowStartTime,
                'windowEndTime' => $windowEndTime,
                'windowStartText' => self::timestampToHTMLInputText($windowStartTime),
                'windowEndText' => self::timestampToHTMLInputText($windowEndTime),
                'channelStats' => $channelStats,
                'emoteStats' => $emoteStats,
                'emoteStatsWindowStartOccurrences' => $minEmoteOccurrences,
                'recentChatters' => $recentChatters,
                'recentChattersMore' => $recentChattersMore,
                'recentEmotes' => $recentEmotes,
                'recentEmotesMore' => $recentEmotesMore
            ]);
        })->bind('channel');

        /**
         * Emotes leaderboard
         */
        $route->get('/channel/{channel}/emotes', function(Request $request, $channel) use($app, $db) {
            // Real occurrences (including all chatters)
            $stmt = $db->prepare("SELECT emotes.emote, type, occurrences FROM emotes
                                LEFT JOIN (SELECT channel, emote, occurrences FROM emote_stats
                                        WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND timestamp = 0) es
                                    ON es.emote=emotes.emote
                                WHERE occurrences > 0
                                ORDER BY es.occurrences DESC");

            $res = $stmt->execute(array(':channel' => $channel));
            if ($res === false)
                $app->abort(500, "Query error: " . self::getStmtErrorMessage($stmt));

            $emotes = [];
            while ($row = $stmt->fetch())
            {
                $emotes[$row['emote']] = [
                    'type' => $row['type'],
                    'real_occurrences' => $row['occurrences'],
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
                'channel' => $channel,
                'emotes' => $emotes
            ]);
        })->bind('emotes');

        /**
         * User Leaderboard for an emote
         */
        $route->get('/channel/{channel}/emote/{emote}', function(Request $request, $channel, $emote) use($app, $db) {
            die("Disabled");
            
            // Determine visualized window bounds
            $shownPeriodValue = $request->query->get('shownPeriodValue', 0);
            $shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');

            $windowEndTime = self::getCurrentTimestamp();
            if ($shownPeriodValue <= 0)
                $windowStartTime = 1;
            else
                $windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;

            // Get emote metadata
            $stmt = $db->prepare("SELECT emote, type FROM ".self::EMOTES_TABLE." WHERE emote = :emote LIMIT 1");
            $stmt->execute(array(':emote' => $emote));
            if ($stmt->rowCount() == 0)
                $app->abort(404, "Emote not found");

            $emoteType = $stmt->fetch()['type'];

            // Get emote stats
            $stmt = $db->prepare("SELECT timestamp, total_occurrences FROM ".self::EMOTE_STATS_TABLE
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND emote = :emote"
                            . "    AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " ORDER BY timestamp ASC");

            $res = $stmt->execute(array(':channel' => $channel, ':emote' => $emote, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query emote stats: " . self::getDBErrorMessage($db));

            $stats = $stmt->fetchAll();
            $minOccurrences = $stats[0]['total_occurrences'];
            $stats = self::resampleTimeSeries($stats, 'total_occurrences', self::DEFAULT_SERIES_RESOLUTION);

            // Get total occurrences
            $stmt = $db->prepare("SELECT SUM(total_occurrences) FROM ("
                            . "   SELECT MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
                            . "   WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND emote = :emote"
                            . "      AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . "   GROUP BY username) a");

            $res = $stmt->execute(array(':channel' => $channel, ':emote' => $emote, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query total occurrences: " . self::getDBErrorMessage($db));

            $totalOccurences = $stmt->fetch()[0];

            // Leaderboard
            $stmt = $db->prepare("SELECT username, MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND emote = :emote"
                            . "    AND username NOT IN " . self::getExcludedChattersTuple() . " AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " GROUP BY username ORDER BY MAX(total_occurrences) DESC LIMIT 1000");

            $res = $stmt->execute(array(':channel' => $channel, ':emote' => $emote, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query leaderboard: " . self::getDBErrorMessage($db));

            $leaderboard = [];
            while ($row = $stmt->fetch())
            {
                $row['percentage'] = 100.0 * ($row['total_occurrences'] / $totalOccurences);
                $leaderboard[] = $row;
            }

            // Trim window range to actual data
            $windowStartTime = $stats[0]['timestamp'];
            $windowEndTime = $stats[max(0, count($stats) - 1)]['timestamp'];

            return $app['twig']->render('emote.twig', [
                'channel' => $channel,
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
        })->bind('emote')
          ->assert('emote', '.+');

        /**
         * Per-user stats for an emote
         */
        $route->get('/channel/{channel}/emote/{emote}/user/{username}', function(Request $request, $channel, $emote, $username) use($app, $db) {
            // Determine visualized window bounds
            $shownPeriodValue = $request->query->get('shownPeriodValue', 0);
            $shownPeriodUnit = $request->query->get('shownPeriodUnit', 'hours');

            $windowEndTime = self::getCurrentTimestamp();
            if ($shownPeriodValue <= 0)
                $windowStartTime = 1;
            else
                $windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;

            // Get stats
            $stmt = $db->prepare("SELECT timestamp, total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND emote = :emote"
                            . "     AND username = :username AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " ORDER BY timestamp ASC");

            $res = $stmt->execute(array(
                    ':channel' => $channel,
                    ':emote' => $emote,
                    ':username' => $username,
                    ':windowStartTime' => $windowStartTime,
                    ':windowEndTime' => $windowEndTime));

            if ($res === false)
                $app->abort(500, "Could not query per-user emote stats: " . self::getDBErrorMessage($db));

            $stats = self::resampleTimeSeries($stmt->fetchAll(), 'total_occurrences', self::DEFAULT_SERIES_RESOLUTION);

            // Trim window range into data range
            $windowStartTime = $stats[0]['timestamp'];
            $windowEndTime = $stats[max(0, count($stats) - 1)]['timestamp'];

            return $app['twig']->render('user_emote.twig', [
                'channel' => $channel,
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
        $route->get('/channel/{channel}/users', function(Request $request, $channel) use($app, $db) {
            $excluded_users = ["nightbot"];
            $max_rank = $request->query->get('max', 100);

            $stmt = $db->prepare("SELECT username, messages FROM ".self::USER_STATS_TABLE
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND timestamp = 0"
                            . " ORDER BY messages DESC LIMIT :limit");

            $res = $stmt->execute(array(':channel' => $channel, ':limit' => $max_rank + count(self::EXCLUDED_CHATTERS)));
            if ($res === false)
                $app->abort(500, "Could not query leaderboard: " . self::getDBErrorMessage($db));

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
                'channel' => $channel,
                'users' => $leaderboard,
                'shownRanks' => $max_rank
            ]);
        })->bind('users');

        /**
         * User message count
         */
        $route->get('/channel/{channel}/user/{username}', function(Request $request, $channel, $username) use($app, $db) {
            // Determine visualized window bounds
            $shownPeriodValue = $request->query->get('shownPeriodValue', 1);
            $shownPeriodUnit = $request->query->get('shownPeriodUnit', 'months');

            $windowEndTime = self::getCurrentTimestamp();
            if ($shownPeriodValue <= 0)
                $windowStartTime = 1;
            else
                $windowStartTime = $windowEndTime - $shownPeriodValue * self::getTimeUnitSecondsMultiplier($shownPeriodUnit) * 1000;

            // User message count stats
            $stmt = $db->prepare("SELECT timestamp, total_messages FROM ".self::USER_STATS_TABLE.""
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND username = :username"
                            . "     AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " ORDER BY timestamp ASC");

            $res = $stmt->execute(array(':channel' => $channel, ':username' => $username, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query user message count stats: " . self::getDBErrorMessage($db));

            if ($stmt->rowCount() == 0)
                $app->abort(404, "No records found for this user in the given time range.");

            $stats = self::resampleTimeSeries($stmt->fetchAll(), 'total_messages', self::DEFAULT_SERIES_RESOLUTION);
            if ($windowStartTime == 0)
                $windowStartTime = $stats[0]['timestamp'];

            // Emote leaderboard for this user
            $stmt = $db->prepare("SELECT emote, MAX(total_occurrences) AS total_occurrences FROM ".self::USER_EMOTE_STATS_TABLE.""
                            . " WHERE channel = :channel AND " . self::getHiddenChannelsCondition() . " AND username = :username"
                            . "     AND timestamp >= :windowStartTime AND timestamp <= :windowEndTime"
                            . " GROUP BY emote ORDER BY MAX(total_occurrences) DESC");

            $res = $stmt->execute(array(':channel' => $channel, ':username' => $username, ':windowStartTime' => $windowStartTime, ':windowEndTime' => $windowEndTime));
            if ($res === false)
                $app->abort(500, "Could not query emote leaderboard for user: " . self::getDBErrorMessage($db));

            $emoteUsages = [];
            while ($row = $stmt->fetch())
                $emoteUsages[$row['emote']] = $row['total_occurrences'];

            // Trim window range to actual data
            $windowStartTime = $stats[0]['timestamp'];
            $windowEndTime = $stats[max(0, count($stats) - 1)]['timestamp'];

            return $app['twig']->render('user.twig', [
                'channel' => $channel,
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
            'millis' => 0.001,
            'seconds' => 1,
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

        $startTime = $startTime ?: $first['timestamp'];
        $endTime = $endTime ?: $last['timestamp'];

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
                // Not enough data before the start of the series - clamp into time range
                $result[] = ['timestamp' => $t, $fieldName => $first[$fieldName]];
            }
            else if ($t >= $last['timestamp'])
            {
                // Not enough data after the end of the series - clamp into time range
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
                    $fieldName => round($before[$fieldName] + $k * ($after[$fieldName] - $before[$fieldName]))
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

    static function ratesToCumulativeSums($timeseries, $fieldName, $startValue=0)
    {
        for ($i = 0, $val = $startValue; $i < count($timeseries); ++$i) {
            $val += $timeseries[$i][$fieldName];
            $timeseries[$i][$fieldName] = $val;
        }

        return $timeseries;
    }

    /**
     * Checks whether the given timeseries is empty or not.
     * If it is, a "minimal" time series with two samples is returned: One with startTime and one with endTime,
     * both having the same constant value of startAndEndValue
     */
    static function checkEmptyTimeseries($timeseries, $valueFieldName, $startTime, $endTime, $defaultStartAndEndValue=0) {
        if (count($timeseries) > 0) {
            return $timeseries;
        } else {
            return [
                [ 'timestamp' => $startTime, $valueFieldName => $defaultStartAndEndValue ],
                [ 'timestamp' => $endTime, $valueFieldName => $defaultStartAndEndValue ]
            ];
        }
    }

    static function getDBErrorMessage($db)
    {
        return $db->errorInfo()[2];
    }

    static function getStmtErrorMessage($stmt)
    {
        return $stmt->errorInfo()[2];
    }


    const HTML_INPUT_DATETIME_FORMAT = "Y-m-d\TH:i";

    // Converts the given timestamp (in milliseconds) to the string format that can be used as a value for
    // the HTML datetime-local date-time picker input field
    static function timestampToHTMLInputText($timestamp)
    {
        $date = new DateTime();
        $date->setTimestamp(round($timestamp * 0.001));
        return $date->format(self::HTML_INPUT_DATETIME_FORMAT);
    }

    // Parses the string value of the HTML datetime-local date-time picker input field and returns
    // the matching timestamp in milliseconds.
    static function htmlInputTextToTimestamp($text)
    {
        $date = DateTime::createFromFormat(self::HTML_INPUT_DATETIME_FORMAT, $text);
        return $date->getTimestamp() * 1000;
    }


    private static function getHiddenChannelsCondition($column = 'channel')
    {
        return $column." IN (SELECT channel FROM ".self::CHANNELS_TABLE." WHERE hidden IS false)";
    }
}
