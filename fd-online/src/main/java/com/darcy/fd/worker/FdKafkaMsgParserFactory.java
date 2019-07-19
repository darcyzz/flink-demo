// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.worker;

import com.darcy.fd.mod.msgparser.ModKafkaMsgParserFactory;
import com.darcy.fd.msgparser.AbstractKafkaMsgParserFactory;
import com.darcy.fd.util.BizContants;


public class FdKafkaMsgParserFactory {

	public static AbstractKafkaMsgParserFactory getFactory(BizContants.Module module) {

		switch (module) {
			case MOD:
				return new ModKafkaMsgParserFactory();
			default:
				return null;
		}
	}
}



