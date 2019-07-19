// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod.flink_indicator;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.indicator.FdIndicatorSink;
import com.darcy.fd.sink.influxdb.InfluxDBPoint;
import com.darcy.fd.util.BizContants;
import com.darcy.fd.util.ModSinkContants;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;

import static com.darcy.fd.util.BizUtils.TeToMsTsLong;


/**
 * @Program: flink-demo
 * @Description: ModGrapAndSink
 * @Author: Darcy(zhangzheng0413 # hotmail.com)
 **/
public class ModIndicatorRequest extends FdIndicatorSink {

	private static final String MODULE_NAME = BizContants.Module.MOD.toString();

	public ModIndicatorRequest(DataStreamManagement outDataStreamManagement) {
		super(outDataStreamManagement);
	}

	// =========================== reqQps2Influxdb
	public void indicatorReqQps2Influxdb() {
		if (!needCalc(MODULE_NAME, Thread.currentThread().getStackTrace()[1].getMethodName())) {
			return;
		}

		DataStream<Row> resInfluxdbDataStream = reqQpsInfluxdb();
		influxdbSink(resInfluxdbDataStream, new ModReqQpsInfluxdb());
	}

	public DataStream<Row> reqQpsInfluxdb() {
		StreamTableEnvironment tableEnv = dataStreamManagement.getTableEnv();

		// 1. from table: mod_wide_table
		Table tableQpsResult = tableEnv.sqlQuery("SELECT TUMBLE_END(rowtime, INTERVAL '1' " +
				"MINUTE) AS ts, " +
				"region, " +
				"indType, " +
				"ntType, " +
				"COUNT(*) AS val " +
				"FROM mod_wide_table " +
				"WHERE indTypeEnumName(indType) ='REQUEST' " +
				"GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE), region, indType, ntType");

		// 2 table to DataStream
		return tableEnv.toAppendStream(tableQpsResult, Row.class);
	}

	public static final class ModReqQpsInfluxdb extends RichMapFunction<Row, InfluxDBPoint> {

		@Override
		public InfluxDBPoint map(Row row) {
			// 1. set tag
			HashMap<String, String> tags = new HashMap<>();
			tags.put("region", row.getField(1).toString());
			tags.put("indType", row.getField(2).toString());
			tags.put("ntType", row.getField(3).toString());

			HashMap<String, Object> fields = new HashMap<>();
			fields.put("val", row.getField(4));

			long timestamp = TeToMsTsLong(row.getField(0));
			return new InfluxDBPoint(ModSinkContants.INFLUXDB_MEASUREMENT_MOD_REQUEST_QPS_WIDE_TABLE,
					timestamp, tags, fields);
		}
	}


}