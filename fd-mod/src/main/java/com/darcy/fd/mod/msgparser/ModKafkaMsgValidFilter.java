// Copyright (C) 2019 Darcy. All rights reserved.


package com.darcy.fd.mod.msgparser;

import com.darcy.fd.model.ModCoreIndMsg;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * * @Program: flink-demo
 * @Description: AbstractKafkaMsgParserFactory
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public final class ModKafkaMsgValidFilter implements FilterFunction<ModCoreIndMsg> {

	@Override
	public boolean filter(ModCoreIndMsg modCoreIndMsg) {
		return modCoreIndMsg.getMsgValid() == 1;
	}

}
