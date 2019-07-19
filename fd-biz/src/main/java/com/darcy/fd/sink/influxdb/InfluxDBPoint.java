// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.sink.influxdb;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a InfluxDB database Point.
 */
public class InfluxDBPoint {

	private String measurement;
	private long timestamp;
	private Map<String, String> tags;
	private Map<String, Object> fields;

	public InfluxDBPoint(String measurement, long timestamp) {
		this.measurement = measurement;
		this.timestamp = timestamp;
		this.fields = new HashMap<>();
		this.tags = new HashMap<>();
	}

	public InfluxDBPoint(String measurement, long timestamp, Map<String, String> tags, Map<String
			, Object> fields) {
		this.measurement = measurement;
		this.timestamp = timestamp;
		this.tags = tags;
		this.fields = fields;
	}

	public String getMeasurement() {
		return measurement;
	}

	public void setMeasurement(String measurement) {
		this.measurement = measurement;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public Map<String, Object> getFields() {
		return fields;
	}

	public void setFields(Map<String, Object> fields) {
		this.fields = fields;
	}
}
