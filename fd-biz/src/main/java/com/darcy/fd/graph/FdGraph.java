// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.graph;

import com.darcy.fd.common.DataStreamManagement;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public abstract class FdGraph {
	public DataStreamManagement dataStreamManagement;

	public StreamTableEnvironment tableEnv;

	public abstract void transAndRegister();

	public void getContext() {
		tableEnv = dataStreamManagement.getTableEnv();
	}

}


