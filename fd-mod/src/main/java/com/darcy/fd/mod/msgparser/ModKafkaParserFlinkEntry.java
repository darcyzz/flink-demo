// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod.msgparser;

import com.alibaba.fastjson.JSONObject;
import com.darcy.fd.model.ModCoreIndMsg;
import com.darcy.fd.msgparser.FdKafkaMsgParser;
import com.darcy.fd.util.BizContants;
import com.darcy.fd.util.BizUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * @Program: flink-demo
 * @Description: ModKafkaParserFlinkEntry
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public final class ModKafkaParserFlinkEntry implements MapFunction<String, ModCoreIndMsg> {

	static final Logger logger = LoggerFactory.getLogger(ModKafkaParserFlinkEntry.class);

	private static final long serialVersionUID = 1L;
	private ModKafkaMsgParserFactory modKafkaMsgParserFactory;

	public ModKafkaParserFlinkEntry(ModKafkaMsgParserFactory modKafkaMsgParserFactory) {
		this.modKafkaMsgParserFactory = modKafkaMsgParserFactory;
	}

	@Override
	public ModCoreIndMsg map(String value) {
		JSONObject jsonObject = JSONObject.parseObject(value);

		try {
			// 1. get message_time, like: 20181120165636
			String messageTime = jsonObject.getString("time");
			if (!BizUtils.IsTodayByDayString(messageTime)) {
				throw new NullPointerException("msgDay is not today");
			}

			// 2. get url, must like: mod/request
			String url = jsonObject.getString("url");

			String handleName = getModHandleName(url);
			if (handleName.equals(BizContants.ERR_HANDLE_NAME)) {
				throw new NullPointerException("getModHandleName error");
			}

			// 3. route process
			FdKafkaMsgParser modKafkaMsgParser =
					modKafkaMsgParserFactory.getMsgHandler(handleName);
			modKafkaMsgParser.destroy();

			// may throw NullPointerException
			ModCoreIndMsg modCoreIndMsg = (ModCoreIndMsg) modKafkaMsgParser.process(jsonObject);
			if (modCoreIndMsg != null) {
				return modCoreIndMsg;
			} else {
				throw new NullPointerException("KafkaMsgParser.process res is null");
			}

		} catch (Exception e) {
			logger.warn("ParseIndMsg error, {}", e.toString());
			return ModCoreIndMsg.builder().msgValid(0).build();
		}
	}

	public static String getModHandleName(String url) {
		String[] urlArr = url.split("/");
		if (urlArr.length != 2) {
			logger.error("getModHandleName error, num of fields must be 2, but: {}", url);
			return BizContants.ERR_HANDLE_NAME;
		}

		String moduleName = urlArr[0];
		if (!moduleName.equalsIgnoreCase(BizContants.Module.MOD.toString())) {
			logger.error("getModHandleName error, module must be MOD, but: {}", moduleName);
			return BizContants.ERR_HANDLE_NAME;
		}

		String handleNameUpperCase = urlArr[1].toUpperCase();
		if (!handleNameUpperCase.equals(BizContants.IndType.REQUEST.toString())) {
			logger.error("getModHandleName error, handle must be REQUEST/...., " +
					"invalid: {}", handleNameUpperCase);

			return BizContants.ERR_HANDLE_NAME;
		}

		return handleNameUpperCase;
	}
}




