// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.model;

import lombok.*;

import java.lang.reflect.Field;

import static com.darcy.fd.util.BizContants.*;


/**
 * @Program: ModCoreIndMsg
 * @Description: common pojo for mod request/... indicator
 * @Author: Darcy(zhangzheng0413#hotmail.com)
 **/
@AllArgsConstructor
@Builder
@Getter
@NoArgsConstructor
@Setter
@ToString
public class ModCoreIndMsg extends CoreIndMsg {

	// MOD REQUEST
	/**
	 * 毫秒时间戳
	 */
	private long eventTimeMs;

	/**
	 * 服务部署，所属地区
	 */
	private Region region;

	/**
	 * 指标类型
	 */
	private IndType indType;

	/**
	 * 网络类型
	 */
	private String ntType;

	/**
	 * 毫秒时间戳，截取到分钟
	 */
	private long eventTsMin;

	/**
	 * msg valid
	 */
	private int msgValid;

	public static String getOrderFieldsString() {
		String orderFieldsString = "";

		Field[] clsFieldsList = ModCoreIndMsg.class.getDeclaredFields();

		for (Field field : clsFieldsList) {
			String fieldName = field.getName();
			orderFieldsString += (fieldName + ",");
		}
		return orderFieldsString.substring(0, orderFieldsString.length() - 1);
	}
}
