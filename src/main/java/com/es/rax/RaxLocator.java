package com.es.rax;

import java.util.HashMap;
import java.util.Map;

public enum RaxLocator {
	TENANT_ID("ac"),
	ENTITY_ID("en"),
	CHECK_ID("ch"),
	METRIC("dim0.");
	RaxLocator(String p) {
		this.prefix = p;
	}
	public String getPrefix() {
		return this.prefix;
	}
	public static Map<String, Object> generateRaxLocatordata(String entityId, String checkId, String metric) {
		Map<String, Object> map = new HashMap<String,Object>();
		map.put(ENTITY_ID.toString(), ENTITY_ID.getPrefix()+entityId);
		map.put(CHECK_ID.toString(), CHECK_ID.getPrefix()+checkId);
		map.put(METRIC.toString(), METRIC.getPrefix()+metric);
		return map;
	}
	private String prefix;
	
}
