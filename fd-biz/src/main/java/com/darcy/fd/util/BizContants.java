// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.util;

/**
 * * @Program: flink-demo
 * @Description: constants for biz[MOD/...]
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
public class BizContants {

	public static final String ERR_HANDLE_NAME = "ERR_HANDLE_NAME";
	public static final int MSG_EXPIRED_MINUTES = 10;

	/**
	 * 模块类型
	 */
	public enum Module {
		MOD
	}

	/**
	 * 服务部署，所属地区
	 */
	public enum Region {
		AP,
		EU,
		US
	}

	/**
	 * 服务部署，所属地区
	 */
	public enum IndType {
		REQUEST
	}
}
