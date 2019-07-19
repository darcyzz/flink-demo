// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.common;

import com.darcy.fd.graph.FdGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * * @Program: flink-demo
 * @Description: StreamRegister
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public abstract class FdAbstractFlinkWorker {

	public DataStreamManagement dataStreamManagement;

	public StreamTableEnvironment tableEnv;
	public DataStream<String> kafkaStream;
	public List<FdGraph> fdGraphs;

	public FdAbstractFlinkWorker(DataStreamManagement dataStreamManagement) {
		this.dataStreamManagement = dataStreamManagement;

		tableEnv = dataStreamManagement.getTableEnv();
	}

	public abstract void parse();

	public abstract void register();

	public abstract void calcAndSink() throws IllegalAccessException, InvocationTargetException;

	public void plan() throws IllegalAccessException, InvocationTargetException {

		parse();

		register();

		// calc and sink
		calcAndSink();
	}
}