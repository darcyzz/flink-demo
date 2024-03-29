// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.sink.influxdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Sink to save data into a InfluxDB cluster.
 */
public class InfluxDBSink extends RichSinkFunction<InfluxDBPoint> {

	private transient InfluxDB influxDBClient;
	private final InfluxDBConfig influxDBConfig;

	/**
	 * Creates a new {@link InfluxDBSink} that connects to the InfluxDB server.
	 *
	 * @param influxDBConfig The configuration of {@link InfluxDBConfig}
	 */
	public InfluxDBSink(InfluxDBConfig influxDBConfig) {
		this.influxDBConfig = Preconditions.checkNotNull(influxDBConfig, "InfluxDB client config should not be null");
	}

	/**
	 * Initializes the connection to InfluxDB by either cluster or sentinels or single server.
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());

		if (!influxDBClient.databaseExists(influxDBConfig.getDatabase())) {
			if (influxDBConfig.isCreateDatabase()) {
				influxDBClient.createDatabase(influxDBConfig.getDatabase());
			} else {
				throw new RuntimeException("This " + influxDBConfig.getDatabase() + " database does not exist!");
			}
		}

		influxDBClient.setDatabase(influxDBConfig.getDatabase());

		if (influxDBConfig.getBatchActions() > 0) {
			influxDBClient.enableBatch(influxDBConfig.getBatchActions(), influxDBConfig.getFlushDuration(), influxDBConfig.getFlushDurationTimeUnit());
		}

		if (influxDBConfig.isEnableGzip()) {

			influxDBClient.enableGzip();
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to InfluxDB.
	 *
	 * @param dataPoint {@link InfluxDBPoint}
	 */
	@Override
	public void invoke(InfluxDBPoint dataPoint, Context context) throws Exception {
		if (StringUtils.isNullOrWhitespaceOnly(dataPoint.getMeasurement())) {
			throw new RuntimeException("No measurement defined");
		}

		Point.Builder builder = Point.measurement(dataPoint.getMeasurement())
				.time(dataPoint.getTimestamp(), TimeUnit.NANOSECONDS);

		if (!CollectionUtil.isNullOrEmpty(dataPoint.getFields())) {
			builder.fields(dataPoint.getFields());
		}

		if (!CollectionUtil.isNullOrEmpty(dataPoint.getTags())) {
			builder.tag(dataPoint.getTags());
		}

		Point point = builder.build();
		influxDBClient.write(point);
	}

	@Override
	public void close() {
		if (influxDBClient.isBatchEnabled()) {
			influxDBClient.disableBatch();
		}
		influxDBClient.close();
	}
}
