// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.udf;

import com.darcy.fd.util.BizContants;
import org.apache.flink.table.functions.ScalarFunction;

public class IndTypeEnumName extends ScalarFunction {

	public String eval(BizContants.IndType indType) {
		return indType.name();
	}
}



