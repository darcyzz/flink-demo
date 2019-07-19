// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod;

import com.darcy.fd.mod.flink_indicator.ModIndicatorRequest;
import com.darcy.fd.mod.msgparser.ModKafkaMsgParserFactory;
import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.udf.IndTypeEnumName;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.*;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Integration tests for streaming SQL.
 */
public class ModRequestTest {

	private static ModFlinkWorker modFlinkWorker;

	private static String eSampleString;
	private static String nDateTs;
	private static String nDateTime;

	private static Properties properties;
	private static DataStreamManagement dataStreamManagement;
	private static String expectedString;
	private static CollectSink collectSink;
	private static final SimpleDateFormat msgSimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	private static ModRequestTest modRequestTest;
	private static ModKafkaMsgParserFactory modKafkaMsgParserFactory;

	@BeforeClass
	public static void preTest() throws Exception {
		modRequestTest = new ModRequestTest();

		eSampleString = "{\"url\": \"mod/request\", \"request\": {\"path\": \"HTTP\", " +
				"\"value\": {\"MODREQUEST_LOG\": {\"region\":\"US\",\"prt\": \"1542756508148\", " +
				"\"nt\": \"1\"}}}, \"time\": \"20181120232828\"}";

		//======================== initContext
		initContext();
	}

	@Before
	public void preSubmit() {
		CollectSink.clear();
		expectedString = null;

		Date today = new Date();
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
		c.setTime(today);
		Date nDate = c.getTime();
		nDateTs = String.valueOf(nDate.getTime());
		nDateTime = msgSimpleDateFormat.format(nDate);
	}

	private static void initContext() throws Exception {
		dataStreamManagement = new DataStreamManagement();

		// 1. prepare env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		tableEnv.registerFunction("indTypeEnumName", new IndTypeEnumName());

		dataStreamManagement.setExecutionEnv(env);
		dataStreamManagement.setTableEnv(tableEnv);

		// 2. transformation
		modKafkaMsgParserFactory = new ModKafkaMsgParserFactory();
		dataStreamManagement.setConfFile(System.getProperty("user.dir") + "/src/test/resources" +
				"/online_mod.properties");
		dataStreamManagement.parseConf();

		collectSink = new CollectSink();
	}

	// sink for test
	private static class CollectSink implements SinkFunction<Row> {
		public static List<String> values = new ArrayList<>();

		public synchronized void invoke(Row value) {
			int len = value.getArity() - 1;
			Row newRow = new Row(len);
			for (int i = 0; i < len; i++) {
				newRow.setField(i, value.getField(i + 1));
			}
			values.add(newRow.toString());
			System.err.println("zheng.zhang Format CollectSink: " + newRow.toString());
			System.err.println("---zheng.zhang CollectSink Real: " + value.toString());
			// System.err.println("---zheng.zhang TeToMsTsLong Real: " + String.valueOf(TeToMsTsLong(value.getField(0))));
		}

		public static void clear() {
			values.clear();
		}
	}

	@Test
	public void testNotModMsg() throws Exception {

		StreamExecutionEnvironment env = dataStreamManagement.getExecutionEnv();

		// 1. prepare data
		String testEle = eSampleString.replace("mod/request", "touchpal/request").replace(
				"20170703100000", nDateTime);

		// 2. load data
		DataStream<String> dataStream = env.fromElements(testEle);

		// 3. init ModFlinkWorker
		modFlinkWorker = new ModFlinkWorker(dataStreamManagement, dataStream,
				modKafkaMsgParserFactory);
		modFlinkWorker.parse();
		modFlinkWorker.register();

		// 4. calc
		ModIndicatorRequest modIndicatorRequest =
				modFlinkWorker.getModCalcAndSink().getModIndicatorRequest();
		DataStream<Row> resRows = modIndicatorRequest.reqQpsInfluxdb();

		resRows.addSink(collectSink);

		// execute
		env.execute("ModRequestTest with msg example");
	}

	@Test
	public void testMsgTimeNotToday() throws Exception {

		StreamExecutionEnvironment env = dataStreamManagement.getExecutionEnv();

		// 1. prepare data
		// NO NEED

		// 2. load data
		DataStream<String> dataStream = env.fromElements(eSampleString);

		// 3. init ModFlinkWorker
		modFlinkWorker = new ModFlinkWorker(dataStreamManagement, dataStream,
				modKafkaMsgParserFactory);
		modFlinkWorker.parse();
		modFlinkWorker.register();

		// 4. calc
		ModIndicatorRequest modIndicatorRequest =
				modFlinkWorker.getModCalcAndSink().getModIndicatorRequest();
		DataStream<Row> resRows = modIndicatorRequest.reqQpsInfluxdb();

		resRows.addSink(collectSink);

		// execute
		env.execute("ModRequestTest with msg example");
	}

	@Test
	public void testMsgPrtNotToday() throws Exception {

		StreamExecutionEnvironment env = dataStreamManagement.getExecutionEnv();

		// 1. prepare data
		// NO NEED
		String testEle = eSampleString.replace("20181120232828", nDateTime);

		// 2. load data
		DataStream<String> dataStream = env.fromElements(testEle);

		// 3. init ModFlinkWorker
		modFlinkWorker = new ModFlinkWorker(dataStreamManagement, dataStream,
				modKafkaMsgParserFactory);
		modFlinkWorker.parse();
		modFlinkWorker.register();

		// 4. calc
		ModIndicatorRequest modIndicatorRequest =
				modFlinkWorker.getModCalcAndSink().getModIndicatorRequest();
		DataStream<Row> resRows = modIndicatorRequest.reqQpsInfluxdb();

		resRows.addSink(collectSink);

		// execute
		env.execute("ModRequestTest with msg example");
	}

	@Test(expected = JobExecutionException.class)
	public void testNotModMsgHandler() throws Exception {

		StreamExecutionEnvironment env = dataStreamManagement.getExecutionEnv();

		// 1. prepare data
		// NO NEED
		String testEle = eSampleString.replace("mod/request", "mod/zhangzheng").replace(
				"20181120232828", nDateTime);

		// 2. load data
		DataStream<String> dataStream = env.fromElements(testEle);

		// 3. init ModFlinkWorker
		modFlinkWorker = new ModFlinkWorker(dataStreamManagement, dataStream,
				modKafkaMsgParserFactory);
		modFlinkWorker.parse();
		modFlinkWorker.register();

		// 4. calc
		ModIndicatorRequest modIndicatorRequest =
				modFlinkWorker.getModCalcAndSink().getModIndicatorRequest();
		DataStream<Row> resRows = modIndicatorRequest.reqQpsInfluxdb();

		resRows.addSink(collectSink);

		// execute
		env.execute("ModRequestTest with msg example");
	}

	@Test
	public void testReqQpsInfluxdb() throws Exception {

		StreamExecutionEnvironment env = dataStreamManagement.getExecutionEnv();

		// 1. prepare data
		// NO NEED
		String testEle =
				eSampleString.replace("1542756508148", nDateTs).replace("20181120232828", nDateTime);

		// 2. load data
		DataStream<String> dataStream = env.fromElements(testEle);

		// 3. init ModFlinkWorker
		modFlinkWorker = new ModFlinkWorker(dataStreamManagement, dataStream,
				modKafkaMsgParserFactory);
		modFlinkWorker.parse();
		modFlinkWorker.register();

		// 4. calc
		ModIndicatorRequest modIndicatorRequest =
				modFlinkWorker.getModCalcAndSink().getModIndicatorRequest();
		DataStream<Row> resRows = modIndicatorRequest.reqQpsInfluxdb();

		resRows.addSink(collectSink);

		// execute
		env.execute("ModRequestTest with msg example");

		// expected data
		expectedString = "US,REQUEST,1,1";

		Assert.assertTrue(CollectSink.values.contains(expectedString));
	}
}
