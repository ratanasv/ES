package com.es.type;

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
	private String prefix;
}
