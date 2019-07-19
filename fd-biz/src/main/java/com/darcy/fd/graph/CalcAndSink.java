// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.graph;

import lombok.Getter;

import java.lang.reflect.InvocationTargetException;

/**
 * * @Program: flink-demo
 * @Description: ModGrapAndSink
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
@Getter
public abstract class CalcAndSink {
	public abstract void graphAndSink() throws IllegalAccessException, InvocationTargetException;
}