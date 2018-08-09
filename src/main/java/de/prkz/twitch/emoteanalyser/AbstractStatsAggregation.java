package de.prkz.twitch.emoteanalyser;

import de.prkz.twitch.emoteanalyser.output.DBOutputFormat;
import de.prkz.twitch.emoteanalyser.output.OutputStatement;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractStatsAggregation<INPUT, KEY, STATS extends AbstractStats>
		extends ProcessWindowFunction<INPUT, STATS, KEY, TimeWindow> {

	protected static final long LATEST_TOTAL_TIMESTAMP = 0;

	private transient ValueState<STATS> statsState;
	protected transient Connection conn;
	private String jdbcUrl;
	private long aggregationIntervalMillis;

	public AbstractStatsAggregation(String jdbcUrl, long aggregationIntervalMillis) {
		this.jdbcUrl = jdbcUrl;
		this.aggregationIntervalMillis = aggregationIntervalMillis;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		statsState = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"stats", getStatsTypeInfo()));

		conn = DriverManager.getConnection(jdbcUrl);
	}

	@Override
	public void process(KEY key,
						Context context,
						Iterable<INPUT> elements,
						Collector<STATS> collector) throws Exception {

		STATS stats = statsState.value();
		if (stats == null)
			stats = createNewStatsForKey(key);

		processWindowElements(stats, elements);

		stats.timestamp = context.window().getEnd();
		collector.collect(stats);
		statsState.update(stats);
	}

	@Override
	public void close() throws Exception {
		if (conn != null)
			conn.close();
	}

	public void aggregateAndExportFrom(DataStream<INPUT> inputStream) {
		DBOutputFormat outputFormat;
		try {
			 outputFormat = DBOutputFormat
					.buildDBOutputFormat()
					.withDriverClass(org.postgresql.Driver.class.getCanonicalName())
					.withJdbcUrl(jdbcUrl)
					.withBatchSize(1)
					.finish();
		}
		catch (Exception ex) {
			throw new RuntimeException("Could not create output format", ex);
		}

		inputStream
				.keyBy(createKeySelector())
				.window(TumblingEventTimeWindows.of(Time.milliseconds(aggregationIntervalMillis)))
				.process(this)
				.flatMap(new FlatMapFunction<STATS, OutputStatement>() {
					@Override
					public void flatMap(STATS stats, Collector<OutputStatement> collector) throws Exception {
						prepareStatsForOutput(stats).forEach(stmt -> collector.collect(stmt));
					}
				})
				.writeUsingOutputFormat(outputFormat);
	}

	protected abstract TypeInformation<STATS> getStatsTypeInfo();

	protected abstract KeySelector<INPUT, KEY> createKeySelector();

	protected abstract STATS createNewStatsForKey(KEY key) throws SQLException;

	protected abstract void processWindowElements(STATS stats, Iterable<INPUT> elements);

	public abstract void prepareTable(Statement stmt) throws SQLException;

	protected abstract Iterable<OutputStatement> prepareStatsForOutput(STATS stats);
}
