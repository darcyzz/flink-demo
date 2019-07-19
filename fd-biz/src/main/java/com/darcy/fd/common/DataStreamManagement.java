// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.common;

import com.darcy.fd.model.ModCoreIndMsg;
import com.darcy.fd.msgparser.AbstractKafkaMsgParserFactory;
import com.darcy.fd.util.BizContants.Module;
import com.darcy.fd.util.BizContants.Region;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Setter
@Getter
public class DataStreamManagement {
	static final Logger logger = LoggerFactory.getLogger(DataStreamManagement.class);

	private String confFile;
	private ParameterTool paramsTool;

	private StreamExecutionEnvironment executionEnv;
	private StreamTableEnvironment tableEnv;

	private Module module;
	private Region region;

	private Map<String, DataStreamEntity> dataStreamEntityMap = new HashMap<>();
	private Map<String, AbstractKafkaMsgParserFactory> msgParserMap = new HashMap<>();

	private SplitStream<ModCoreIndMsg> modSplitStream;

	@Getter
	public class DataStreamEntity {
		private DataStream<Row> dataStream;
		private String tableName;

		public DataStreamEntity(DataStream<Row> outDataStream, String outTableName) {
			dataStream = outDataStream;
			tableName = outTableName;
		}
	}

	public void setEntity(DataStream<Row> dataStream, String tableName) {
		DataStreamEntity dataStreamEntity = new DataStreamEntity(dataStream, tableName);
		dataStreamEntityMap.put(tableName, dataStreamEntity);

		showSchema(tableName);
	}

	public void showSchema(String tableName) {
		TableSchema schema = tableEnv.scan(tableName).getSchema();
		System.out.printf("---Table Columns of <%s> : %s\n", tableName, schema.toString());
	}

	public void parseConf() throws IOException {
		paramsTool = ParameterTool.fromPropertiesFile(confFile);
		paramsTool.getProperties().list(System.out);

		// 1. region
		if (paramsTool.has("region")) {
			region = Region.valueOf(paramsTool.get("region"));
			logger.info("parseParamsTool region[{}]", region);
		} else {
			logger.error("parseParamsTool region empty");
			System.exit(0);
		}

		// 2. module
		if (paramsTool.has("module")) {
			module = Module.valueOf(paramsTool.get("module"));
			logger.info("parseParamsTool modules[{}]", module);
		} else {
			logger.error("parseParamsTool module empty");
			System.exit(0);
		}
	}
}



