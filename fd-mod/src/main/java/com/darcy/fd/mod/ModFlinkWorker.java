// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod;

import com.darcy.fd.graph.CalcAndSink;
import com.darcy.fd.mod.flink_graph.ModCalcAndSink;
import com.darcy.fd.mod.msgparser.ModEventTimeExtractor;
import com.darcy.fd.mod.msgparser.ModKafkaMsgParserFactory;
import com.darcy.fd.mod.msgparser.ModKafkaMsgValidFilter;
import com.darcy.fd.mod.msgparser.ModKafkaParserFlinkEntry;
import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.common.FdAbstractFlinkWorker;
import com.darcy.fd.model.ModCoreIndMsg;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;

import java.lang.reflect.InvocationTargetException;


/**
 * * @Program: flink-demo
 * @Description: ModGrapAndSink
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
@Getter
@Setter
public class ModFlinkWorker extends FdAbstractFlinkWorker {

	private ModCalcAndSink modCalcAndSink;
	private DataStream<ModCoreIndMsg> mergedDataStream;

	private ModKafkaMsgParserFactory modKafkaMsgParserFactory;


	public ModFlinkWorker(DataStreamManagement dataStreamManagement,
	                      DataStream<String> inKafkaStream,
	                      ModKafkaMsgParserFactory modKafkaMsgParserFactory) {
		super(dataStreamManagement);

		this.kafkaStream = inKafkaStream;
		this.modKafkaMsgParserFactory = modKafkaMsgParserFactory;

		this.modCalcAndSink = new ModCalcAndSink(dataStreamManagement);
	}

	@Override
	public void parse() {
		mergedDataStream = kafkaStream.map(new ModKafkaParserFlinkEntry(modKafkaMsgParserFactory))
				.filter(new ModKafkaMsgValidFilter()).assignTimestampsAndWatermarks(new ModEventTimeExtractor(dataStreamManagement));
	}

	@Override
	public void register() {

		tableEnv.registerDataStream("mod_wide_table", mergedDataStream,
				ModCoreIndMsg.getOrderFieldsString() + ", rowtime.rowtime");
		TableSchema reqSchema = tableEnv.scan("mod_wide_table").getSchema();
		System.out.println("---Table Columns of <mod_wide_table> : " + reqSchema.toString());

	}

	@Override
	public void calcAndSink() throws IllegalAccessException, InvocationTargetException {
		// Note: transformation is different from biz products[MOD/...]

		// 1. Module: MOD
		modCalcAndSink.graphAndSink();
	}
}