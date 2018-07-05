package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractStatsAggregation<INPUT, KEY, STATS extends AbstractStats>
		extends ProcessWindowFunction<INPUT, STATS, KEY, TimeWindow> {

	private transient ValueState<STATS> statsState;
	protected transient Connection conn;
	private String jdbcUrl;
	private Time aggregationInterval;

	public AbstractStatsAggregation(String jdbcUrl, Time aggregationInterval) {
		this.jdbcUrl = jdbcUrl;
		this.aggregationInterval = aggregationInterval;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		statsState = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"stats", TypeInformation.of(new TypeHint<STATS>() {})));

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

		for (INPUT element : elements)
			updateStats(stats, element);

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
		JDBCOutputFormat outputFormat = JDBCOutputFormat
				.buildJDBCOutputFormat()
				.setDrivername("org.postgresql.Driver")
				.setDBUrl(jdbcUrl)
				.setBatchInterval(1)
				.setQuery(getInsertSQL())
				.setSqlTypes(getRowColumnTypes())
				.finish();

		inputStream
				.keyBy(createKeySelector())
				.window(TumblingEventTimeWindows.of(aggregationInterval))
				.process(this)
				.map(new MapFunction<STATS, Row>() {
					@Override
					public Row map(STATS stats) throws Exception {
						return getRowFromStats(stats);
					}
				})
				.writeUsingOutputFormat(outputFormat);
	}

	protected abstract KeySelector<INPUT, KEY> createKeySelector();

	protected abstract STATS createNewStatsForKey(KEY key) throws SQLException;

	protected abstract STATS updateStats(STATS stats, INPUT input);

	public abstract void prepareTable(Statement stmt) throws SQLException;

	protected abstract Row getRowFromStats(STATS stats);

	protected abstract String getInsertSQL();

	protected abstract int[] getRowColumnTypes();
}
