// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.common.FdAbstractFlinkWorker;
import com.darcy.fd.msgparser.AbstractKafkaMsgParserFactory;
import com.darcy.fd.udf.IndTypeEnumName;
import com.darcy.fd.util.BizContants.Module;
import com.darcy.fd.util.BizContants.Region;
import com.darcy.fd.worker.FdFlinkWorkerFactory;
import com.darcy.fd.worker.FdKafkaMsgParserFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * @Program: FdOnline
 * @Description: Online Monitor Main Entry
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public class FdOnline {

	private static DataStreamManagement dataStreamManagement;

	static final Logger logger = LoggerFactory.getLogger(FdOnline.class);

	public static void main(String[] args) throws Exception {

		FdOnline fdOnline = new FdOnline();
		dataStreamManagement = new DataStreamManagement();

		// 1. load conf from outside command line
		final ParameterTool cmdParamsTool = ParameterTool.fromArgs(args);
		cmdParamsTool.getProperties().list(System.out);
		if (!cmdParamsTool.has("confFile")) {
			logger.error("FdOnline confFile invalid");
			System.exit(0);
		}
		dataStreamManagement.setConfFile(cmdParamsTool.get("confFile"));
		dataStreamManagement.parseConf();

		// 2. get StreamExecutionEnvironment
		StreamExecutionEnvironment executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(executionEnv);
		// register UDF
		tableEnv.registerFunction("indTypeEnumName", new IndTypeEnumName());

		dataStreamManagement.setTableEnv(tableEnv);

		// 3. enable checkpoint for HA and set other conf
		executionEnv.enableCheckpointing(dataStreamManagement.getParamsTool().getLong("check_point_interval"));
		executionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		dataStreamManagement.setExecutionEnv(executionEnv);

		// 4. get properties
		Properties prop = new Properties();
		Region region = dataStreamManagement.getRegion();
		Module module = dataStreamManagement.getModule();
		String propFile = "/" + module.toString() + "/online_" + region.toString() + ".properties";
		prop.load(fdOnline.getClass().getResourceAsStream(propFile));
		prop.list(System.out);

		// 5. get kafka datasource
		FlinkKafkaConsumer<String> myConsumer =
				new FlinkKafkaConsumer<String>(prop.getProperty("topic"),
						new SimpleStringSchema(), prop);
		// consumer mode: StartupMode.LATEST
		myConsumer.setStartFromLatest();

		String sourceName = module.toString() + "_" + region.toString();
		DataStream<String> kafkaStream = executionEnv.addSource(myConsumer, sourceName);

		// 7. get kafka msg parser factory
		AbstractKafkaMsgParserFactory abstractKafkaMsgParserFactory =
				FdKafkaMsgParserFactory.getFactory(module);
		if (abstractKafkaMsgParserFactory == null) {
			logger.warn("FdOnline args[{}], abstractKafkaMsgParserFactory is null, please check", module.toString());
			System.exit(0);
		}

		// 8. worker for: transformation, register, calc and sink
		FdAbstractFlinkWorker flinkWorker = FdFlinkWorkerFactory.getWorker(module,
				dataStreamManagement, kafkaStream, abstractKafkaMsgParserFactory);
		if (flinkWorker == null) {
			logger.error("FdOnline args[{}], has no worker, please check", module.toString());
			System.exit(0);
		}

		// 9. make graph
		flinkWorker.plan();

		// 10. execute job
		executionEnv.execute("FdOnline-" + sourceName);
	}
}
