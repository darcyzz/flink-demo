// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod.msgparser;

import com.darcy.fd.msgparser.FdKafkaMsgParser;
import com.darcy.fd.util.BizContants;

import static com.darcy.fd.util.BizContants.*;

public abstract class ModKafkaMsgParser extends FdKafkaMsgParser {
	public long eventTimeMs;
	public BizContants.Region region;
	public IndType indType;
	public String ntType;

	public void destroy() {
		eventTimeMs = 0L;
		region = null;
		indType = null;
		ntType = null;

		jsonObject = null;
		coreIndMsg = null;
	}
}
