// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.worker;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.common.FdAbstractFlinkWorker;
import com.darcy.fd.mod.ModFlinkWorker;
import com.darcy.fd.mod.msgparser.ModKafkaMsgParserFactory;
import com.darcy.fd.msgparser.AbstractKafkaMsgParserFactory;
import com.darcy.fd.util.BizContants;
import org.apache.flink.streaming.api.datastream.DataStream;


public class FdFlinkWorkerFactory {

	public static FdAbstractFlinkWorker getWorker(BizContants.Module module,
	                                              DataStreamManagement dataStreamManagement,
	                                              DataStream<String> kafkaStream,
	                                              AbstractKafkaMsgParserFactory abstractKafkaMsgParserFactory) {

		switch (module) {
			case MOD:
				return new ModFlinkWorker(dataStreamManagement, kafkaStream,
						(ModKafkaMsgParserFactory) abstractKafkaMsgParserFactory);
			default:
				return null;
		}
	}
}







