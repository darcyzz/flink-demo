// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.msgparser;

import com.alibaba.fastjson.JSONObject;
import com.darcy.fd.model.CoreIndMsg;

public abstract class FdKafkaMsgParser {

	public JSONObject jsonObject;
	public String regionName;
	public CoreIndMsg coreIndMsg;

	public abstract boolean isLogValid();

	public abstract boolean parseLog();

	public abstract void genRowLog();

	public abstract void destroy();

	public CoreIndMsg process(JSONObject jsonObject) {
		this.jsonObject = jsonObject;

		if (!isLogValid()) {
			return null;
		}

		if (!parseLog()) {
			return null;
		}

		genRowLog();

		return coreIndMsg;
	}
}



