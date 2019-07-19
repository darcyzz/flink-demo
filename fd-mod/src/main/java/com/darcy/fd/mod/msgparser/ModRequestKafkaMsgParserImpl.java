// Copyright (C) 2019 Darcy. All rights reserved.


package com.darcy.fd.mod.msgparser;

import com.alibaba.fastjson.JSONObject;
import com.darcy.fd.model.ModCoreIndMsg;
import com.darcy.fd.util.BizContants;
import com.darcy.fd.util.BizUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * @Program: flink-demo
 *
 * @Description: ModRequestKafkaMsgParserImpl
 * @Author: Darcy(zhangzheng0413 # hotmail.com)
 **/
public class ModRequestKafkaMsgParserImpl extends ModKafkaMsgParser {

	static final Logger logger = LoggerFactory.getLogger(ModRequestKafkaMsgParserImpl.class);

	public static long KafkaLagCount = 0;

	@Override
	public boolean isLogValid() {
		return !jsonObject.getJSONObject("request").getJSONObject("value").getJSONObject("MODREQUEST_LOG").isEmpty();
	}

	@Override
	public boolean parseLog() {
		JSONObject coreJSONObject =
				jsonObject.getJSONObject("request").getJSONObject("value").getJSONObject("MODREQUEST_LOG");

		eventTimeMs = coreJSONObject.getLongValue("prt");
		if (!BizUtils.IsTodayByTimeStamp(eventTimeMs)) {
			KafkaLagCount++;
			if (KafkaLagCount == 50000) {
				logger.error("ModRequestKafkaMsgParserImpl KafkaLagCount: {}", KafkaLagCount);
				KafkaLagCount = 0;
			}

			return false;
		}

		region = BizContants.Region.valueOf(coreJSONObject.getString("region").toUpperCase());
		indType = BizContants.IndType.REQUEST;
		ntType = coreJSONObject.getString("nt");


		return true;
	}

	@Override
	public void genRowLog() {
		coreIndMsg = ModCoreIndMsg.builder()
				.eventTimeMs(eventTimeMs)
				.region(region)
				.indType(indType)
				.ntType(ntType)
				.msgValid(1)
				.build();
	}
}