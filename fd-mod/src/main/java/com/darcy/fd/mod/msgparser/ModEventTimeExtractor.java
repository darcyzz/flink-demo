// Copyright (C) 2019 Darcy. All rights reserved.


package com.darcy.fd.mod.msgparser;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.model.ModCoreIndMsg;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * * @Program: flink-demo
 * @Description: ModEventTimeExtractor
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public final class ModEventTimeExtractor implements AssignerWithPeriodicWatermarks<ModCoreIndMsg> {

	private long maxOutOfOrderness;

	private long currentMaxTimestamp;

	public ModEventTimeExtractor(DataStreamManagement dataStreamManagement) {
		this.maxOutOfOrderness = dataStreamManagement.getParamsTool().getLong(
				"flink_fd_max_outOfOrderness_ms");
	}

	@Override
	public long extractTimestamp(ModCoreIndMsg element, long previousElementTimestamp) {
		long timestamp = element.getEventTimeMs();
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// return the watermark as current highest timestamp minus the out-of-orderness bound
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}
}