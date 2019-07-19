// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.mod.flink_graph;

import com.darcy.fd.common.DataStreamManagement;
import com.darcy.fd.graph.CalcAndSink;
import com.darcy.fd.mod.flink_indicator.*;
import lombok.Getter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * * @Program: flink-demo
 * @Description: ModGrapAndSink
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
@Getter
public class ModCalcAndSink extends CalcAndSink {

	private ModIndicatorRequest modIndicatorRequest;


	public ModCalcAndSink(DataStreamManagement dataStreamManagement) {
		modIndicatorRequest = new ModIndicatorRequest(dataStreamManagement);
	}

	@Override
	public void graphAndSink() throws IllegalAccessException, InvocationTargetException {

		// Mod REQUEST
		reqGrapAndSink();
	}

	public void reqGrapAndSink() throws IllegalAccessException, InvocationTargetException {
		Method[] methods = modIndicatorRequest.getClass().getMethods();

		for (Method method : methods) {
			if (method.getName().startsWith("indicator")) {
				method.invoke(modIndicatorRequest);
			}
		}
	}
}