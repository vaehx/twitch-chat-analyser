package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

// Output is a batch of partial aggregates to be written to a sink
public abstract class AbstractStatsAggregation<INPUT, KEY, STATS extends AbstractStats>
        extends KeyedProcessFunction<Integer, INPUT, List<STATS>> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStatsAggregation.class);

    protected static final long LATEST_TOTAL_TIMESTAMP = 0;

    private transient MapState<Tuple2<KEY, Long>, STATS> aggregates;
    private transient ValueState<Long> nextTriggerTimeState;

    private String jdbcUrl;
    private long aggregationIntervalMillis;
    private long triggerIntervalMillis;

    public AbstractStatsAggregation(String jdbcUrl,
                                    long aggregationIntervalMillis,
                                    long triggerIntervalMillis) {
        this.jdbcUrl = jdbcUrl;
        this.aggregationIntervalMillis = aggregationIntervalMillis;
        this.triggerIntervalMillis = triggerIntervalMillis;
    }

    @Override
    public void open(Configuration config) throws Exception {
        RuntimeContext ctx = getRuntimeContext();

        MapStateDescriptor<Tuple2<KEY, Long>, STATS> aggregatesDesc = new MapStateDescriptor<>(
                "aggregates", getKeyTypeInfo(), getStatsTypeInfo());
        aggregates = ctx.getMapState(aggregatesDesc);

        ValueStateDescriptor<Long> nextTriggerTimeStateDesc = new ValueStateDescriptor<>(
                "nextTriggerTime", LongSerializer.INSTANCE);
        nextTriggerTimeState = ctx.getState(nextTriggerTimeStateDesc);
    }

    @Override
    public void processElement(INPUT element, Context context, Collector<List<STATS>> collector) throws Exception {
        KEY elementKey = getKeyForElement(element);
        if (elementKey == null)
            return;

        long timestamp = getTimestampForElement(element);
        long windowStart = TimeWindow.getWindowStartWithOffset(timestamp, 0, aggregationIntervalMillis);
        long windowEnd = windowStart + aggregationIntervalMillis;

        Tuple2<KEY, Long> key = new Tuple2<>(elementKey, windowEnd);
        STATS stats = aggregates.get(key);
        if (stats == null) {
            stats = createNewStatsForKey(elementKey);
            stats.timestamp = windowEnd;
        }

        stats = aggregate(stats, element);

        aggregates.put(key, stats);

        // Register next trigger timer if it doesn't exist yet
        Long nextTriggerTime = nextTriggerTimeState.value();
        if (nextTriggerTime == null) {
            TimerService timerService = context.timerService();

            nextTriggerTime = timerService.currentProcessingTime() + triggerIntervalMillis;
            timerService.registerProcessingTimeTimer(nextTriggerTime);

            nextTriggerTimeState.update(nextTriggerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<STATS>> out) throws Exception {
        if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
            Long nextTriggerTime = nextTriggerTimeState.value();
            if (nextTriggerTime != null && nextTriggerTime == timestamp) {
                // Emit all current aggregates
                List<STATS> stats = new ArrayList<>();
                for (STATS stat : aggregates.values())
                    stats.add(stat);

                out.collect(stats);

                // Clear current stats
                aggregates.clear();

                // Reset timer
                nextTriggerTimeState.update(null);
            }
        }
    }

    public void aggregateAndExportFrom(DataStream<INPUT> inputStream, int parallelism, String name) {
        // Aggregate into event-time windows, emit partial results every n seconds
        DataStream<List<STATS>> aggregatedStatsBatches = inputStream
                .keyBy(new KeySelector<INPUT, Integer>() {
                    @Override
                    public Integer getKey(INPUT element) {
                        return getHashForElement(element) % parallelism;
                    }
                })
                .process(this)
                .name(name)
                .setParallelism(parallelism)
                .uid(name + "_process_1");

        // Write batches of partial results to database
        /*BatchedPreparedDBOutputFormat outputFormat;
        try {
            outputFormat = BatchedPreparedDBOutputFormat
                    .builder()
                    .withDriverClass(org.postgresql.Driver.class.getCanonicalName())
                    .withJdbcUrl(jdbcUrl)
                    .withSql(getUpsertSql())
                    .withTypesArray(getUpsertTypes())
                    .finish();
        } catch (Exception ex) {
            throw new RuntimeException("Could not create output format", ex);
        }*/

        aggregatedStatsBatches
                .flatMap(new FlatMapFunction<List<STATS>, STATS>() {
                    @Override
                    public void flatMap(List<STATS> statsList, Collector<STATS> collector) throws Exception {
                        for (STATS stats : statsList)
                            collector.collect(stats);
                    }
                })
                .returns(getStatsTypeInfo())
                .addSink(new XAPostgresSink<STATS>(jdbcUrl, 100) {
                    @Override
                    protected String getInsertSQL() {
                        return getUpsertSql();
                    }

                    @Override
                    protected void setFields(PreparedStatement stmt, STATS row) throws SQLException {
                        setFieldsForOutput(stmt, row);
                    }
                })
                .name(name + "_Sink")
                .setParallelism(parallelism)
                .uid(name + "_sink_1");
    }

    /**
     * Escapes string for use in sql statement
     */
    protected static String escapeSingleQuotes(String str) {
        return str.replaceAll("'", "''");
    }

    protected abstract TypeInformation<Tuple2<KEY, Long>> getKeyTypeInfo();

    protected abstract TypeInformation<STATS> getStatsTypeInfo();

    protected abstract long getTimestampForElement(INPUT element);

    protected abstract KEY getKeyForElement(INPUT element);

    protected abstract Integer getHashForElement(INPUT element);

    /**
     * Creates a new (empty) stats aggregate object for the given key
     */
    protected abstract STATS createNewStatsForKey(KEY key);

    /**
     * Iterative aggregate function to update stats with the given element
     * @return either the same stats object or a new stats object
     */
    protected abstract STATS aggregate(STATS stats, INPUT element);

    public abstract void prepareTable(Statement stmt) throws SQLException;

    protected abstract String getUpsertSql();

    /**
     * Sets the fields of the stmt for the given stats, which are specific to the upsert sql and
     * the previously prepared rows.
     */
    protected abstract void setFieldsForOutput(PreparedStatement stmt, STATS stats) throws SQLException;
}
