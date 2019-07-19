// Copyright (C) 2019 Darcy. All rights reserved.


package com.darcy.fd.common;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

/**
 * * @Program: flink-demo
 * @Description: AbstractKafkaMsgParserFactory
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public final class RowEventTimeExtractor implements AssignerWithPeriodicWatermarks<Row> {

	private long maxOutOfOrderness;

	private long currentMaxTimestamp;

	public RowEventTimeExtractor(DataStreamManagement dataStreamManagement) {
		this.maxOutOfOrderness = dataStreamManagement.getParamsTool().getLong(
				"flink_fd_max_outOfOrderness_ms");
	}

	@Override
	public long extractTimestamp(Row row, long previousElementTimestamp) {
		long timestamp = (long) row.getField(0);
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// return the watermark as current highest timestamp minus the out-of-orderness bound
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}
}