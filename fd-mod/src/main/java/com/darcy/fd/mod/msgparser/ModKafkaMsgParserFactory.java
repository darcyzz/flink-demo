// Copyright (C) 2019 Darcy. All rights reserved.


package com.darcy.fd.mod.msgparser;

import com.darcy.fd.msgparser.AbstractKafkaMsgParserFactory;
import com.darcy.fd.util.BizContants;


/**
 * * @Program: flink-demo
 * @Description: AbstractKafkaMsgParserFactory
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public class ModKafkaMsgParserFactory extends AbstractKafkaMsgParserFactory {

	@Override
	public ModKafkaMsgParser getMsgHandler(String modKafkaMsgHandleName) {

		if (modKafkaMsgHandleName == null) {
			return null;
		}

		switch (BizContants.IndType.valueOf(modKafkaMsgHandleName)) {
			case REQUEST:
				return new ModRequestKafkaMsgParserImpl();
			default:
				return null;
		}
	}
}