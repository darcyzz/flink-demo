// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Simple static biz tools.
 *
 * @author Darcy(zhangzheng0413#hotmail.com)
 * @since 2018-11-19 20:18
 **/
public class BizUtils {

	static final Logger logger = LoggerFactory.getLogger(BizUtils.class);
	static Date DateStatic = new Date();
	static SimpleDateFormat SimpleDateFormatES = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static Random random = new Random();

	/**
	 * judge messageTime is today[UTC]
	 *
	 * @param messageTime string of time like: 20181205155359
	 * @return boolean
	 * @throws ParseException if messageTime is empty
	 */
	public static boolean IsTodayByDayString(String messageTime) throws ParseException {
		Date today = new Date();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date msgDay = simpleDateFormat.parse(messageTime);
		if (!DateUtils.isSameDay(today, msgDay)) {
			logger.error("IsTodayByDayString msgDay is not today[{}]", messageTime);
			return false;
		}

		return true;
	}

	/**
	 * judge timeStamp is today[UTC] and it is Last MSG_EXPIRED_MINUTES
	 * minutes
	 *
	 * @param timeStamp long of timstamp msec
	 * @return boolean
	 */
	public static boolean IsTodayByTimeStamp(long timeStamp) {
		Date logTs = new Date(timeStamp);
		Date currentTs = new Date();
		if (!DateUtils.isSameDay(currentTs, logTs)) {
			logger.warn("IsTodayByTimeStamp msgDay is not today[{}]", timeStamp);
			return false;
		}

		int minutes = (int) (currentTs.getTime() - logTs.getTime()) / (60 * 1000);
		if ((minutes < 0) || minutes >= BizContants.MSG_EXPIRED_MINUTES) {
			logger.warn("IsTodayByTimeStamp msg expired, log_ts: {}, " +
					"gap: {} m", logTs.getTime(), minutes);
			return false;
		}

		return true;
	}

	/**
	 * Return upper case region whether main class parameters contain region and whether it is
	 * valid.
	 *
	 * @param args String[]
	 * @return String UpperCase region name of {@link BizContants.Region}
	 */
	public static String CheckRegion(String[] args) {
		String region = "US";

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			region = params.get("region");
		} catch (Exception e) {
			logger.error("No Region specified. Please run '--region <region> --module <module>' " +
					"Note: region can be AP/EU/US");

			System.exit(0);
		}
		if (!StringUtils.equalsIgnoreCase(region, BizContants.Region.AP.name())
				&& !StringUtils.equalsIgnoreCase(region, BizContants.Region.EU.name())
				&& !StringUtils.equalsIgnoreCase(region, BizContants.Region.US.name())) {
			logger.error("Region must be one of AP/EU/US");
			System.exit(0);
		}
		return region.toUpperCase();
	}

	/**
	 * Return upper case module name whether main class parameters contain region and whether it is
	 * valid
	 *
	 * @param args String[]
	 * @return String UpperCase region name of {@link BizContants.Module}
	 */
	public static String CheckModule(String[] args) {
		String module = "MOD";

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			module = params.get("module");
		} catch (Exception e) {
			logger.error("No Module specified. Please run '--module <module> ' " +
					"Note: Module can be MOD/..");
			System.exit(0);
		}
		if (!StringUtils.equalsIgnoreCase(module, BizContants.Module.MOD.name())) {
			logger.error("Module must be one of MOD/...");
			System.exit(0);
		}
		return module.toUpperCase();
	}

	/**
	 * Return IndType whether main class parameters contain IndType and whether it is
	 * valid
	 *
	 * @param args String[]
	 * @return IndType of {@link BizContants.IndType}
	 */
	public static BizContants.IndType CheckIndType(String[] args) {
		BizContants.IndType indType = BizContants.IndType.REQUEST;

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			indType = BizContants.IndType.valueOf(params.get("indtype"));
		} catch (Exception e) {
			logger.error("No IndType specified. Please run '--indtype <IndType> ' " +
					"Note: IndType can be REQUEST/...");
			System.exit(0);
		}

		if (!indType.equals(BizContants.IndType.REQUEST.name())) {
			logger.error("Module must be one of  REQUEST/...Please check!");
			System.exit(0);
		}
		return indType;
	}

	/**
	 * Return IndType whether main class parameters contain IndType and whether it is
	 * valid
	 *
	 * @param args String[]
	 * @return IndType of {@link BizContants.IndType}
	 */
	public static String CheckDay(String[] args) {
		Date msgDay = new Date();
		String day = "";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			day = params.get("day");
			msgDay = simpleDateFormat.parse(day);
		} catch (Exception e) {
			logger.error("No Day specified. Please run '--day <day> ' " +
					"Note: Day format must be like: 20170703.");
			System.exit(0);
		}

		if (msgDay.after(new Date())) {
			logger.error("Day must before today.");
			System.exit(0);
		}

		return day;
	}

	/**
	 * Transfer Timestamp object to string of timestamp(wihch is long) and return.
	 *
	 * @param o Object of Timestamp {@link java.sql.Timestamp}
	 * @return java.lang.String
	 */
	public static String TeToMsTsString(Object o) {
		return SimpleDateFormatES.format((Timestamp) o);
	}

	/**
	 * Transfer Timestamp object to long timestamp and return.
	 *
	 * @param o Object of Timestamp {@link java.sql.Timestamp}
	 * @return long
	 */
	public static long TeToMsTsLong(Object o) {
		return ((Timestamp) o).getTime() * 1000000 + random.nextInt(100000);
	}

	/**
	 * Transfer ms timestamp long to minute long timestamp and return.
	 *
	 * @param timestamp ms long timestamp}
	 * @return long
	 */
	public static long TsToMinuteLong(long timestamp) {
		DateStatic.setTime(timestamp);
		DateStatic.setSeconds(0);
		return DateStatic.getTime();
	}

	/**
	 * Transfer format date to long timestamp and return.
	 *
	 * @param dateFormat format date like: 20181120232828
	 * @return long ms timestamp
	 */
	public static long DateFormatToMs(String dateFormat) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		Date msgDay = simpleDateFormat.parse(dateFormat);
		return msgDay.getTime();
	}
}