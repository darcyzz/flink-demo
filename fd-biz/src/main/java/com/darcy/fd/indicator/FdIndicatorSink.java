// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.indicator;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.sink.influxdb.InfluxDBConfig;
import com.darcy.fd.sink.influxdb.InfluxDBPoint;
import com.darcy.fd.sink.influxdb.InfluxDBSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


/**
 * * @Program: flink-demo
 *
 * @Description: FdIndicatorSink
 * @Author: Darcy(zhangzheng0413 # hotmail.com)
 **/
public abstract class FdIndicatorSink {
	public DataStreamManagement dataStreamManagement;
	public InfluxDBConfig influxDBConfig;

	static final Logger logger = LoggerFactory.getLogger(FdIndicatorSink.class);

	public FdIndicatorSink(DataStreamManagement outDataStreamManagement) {
		dataStreamManagement = outDataStreamManagement;

		// init influxDBConfig
		// batchActions / flushDuration 满足任意一个即可
		influxDBConfig = InfluxDBConfig.builder(
				dataStreamManagement.getParamsTool().get("influxdb_host"),
				dataStreamManagement.getParamsTool().get("influxdb_user"),
				dataStreamManagement.getParamsTool().get("influxdb_password"),
				dataStreamManagement.getParamsTool().get("influxdb_database"))
				.batchActions(dataStreamManagement.getParamsTool().getInt("influxdb_batch_actions"))
				.flushDuration(dataStreamManagement.getParamsTool().getInt("influxdb_flush_duration_ms"), TimeUnit.MILLISECONDS)
				.enableGzip(true)
				.build();

	}

	public void influxdbSink(DataStream<Row> resDataStream,
	                         RichMapFunction<Row, InfluxDBPoint> richMapFunction) {
		if (!dataStreamManagement.getParamsTool().getBoolean("enable_influxdb")) {
			return;
		}
		if (dataStreamManagement.getParamsTool().getBoolean("enable_print")) {
			resDataStream.print();
		}

		// 1. map to InfluxDBPoint
		DataStream<InfluxDBPoint> dataStream = resDataStream.map(richMapFunction);

		// 2 Sink
		try {
			dataStream.addSink(new InfluxDBSink(influxDBConfig)).setParallelism(1);
		} catch (Exception e) {
			logger.error("influxdbSink error[{}], {}", e.toString(), richMapFunction.getClass().getName());
		}
	}

	public boolean needCalc(String moduleName, String methodName) {
		String keyNameEnable = moduleName + "_" + methodName;

		return dataStreamManagement.getParamsTool().getBoolean(keyNameEnable, false);
	}
}