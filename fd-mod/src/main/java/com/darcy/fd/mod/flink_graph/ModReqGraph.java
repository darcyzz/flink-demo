// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod.flink_graph;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.graph.FdGraph;
import com.darcy.fd.mod.msgparser.ModEventTimeExtractor;
import com.darcy.fd.model.ModCoreIndMsg;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;

import static com.darcy.fd.util.BizContants.IndType;

/**
 * * @Program: flink-demo
 *
 * @Description: ModGrapAndSink
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public class ModReqGraph extends FdGraph {

	public ModReqGraph(DataStreamManagement dataStreamManagement) {
		this.dataStreamManagement = dataStreamManagement;
	}

	@Override
	public void transAndRegister() {
		getContext();

		// 1 MOD REQUEST
		DataStream<ModCoreIndMsg> modReqMsgDataStream =
				dataStreamManagement.getModSplitStream().select(IndType.REQUEST.toString()).assignTimestampsAndWatermarks(new ModEventTimeExtractor(dataStreamManagement));
		tableEnv.registerDataStream("mod_req_wide_table", modReqMsgDataStream,
				ModCoreIndMsg.getOrderFieldsString() + ", rowtime.rowtime");
		TableSchema reqSchema = tableEnv.scan("mod_req_wide_table").getSchema();
		System.out.println("---Table Columns of <mod_req_wide_table> : " + reqSchema.toString());

	}
}