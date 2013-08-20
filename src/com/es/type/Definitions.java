package com.es.type;

public enum Definitions {
	TENANT_ID("ac"),
	ENTITY_ID("en"),
	CHECK_ID("ch"),
	METRIC("dim0.");
	Definitions(String p) {
		this.prefix = p;
	}
	public String getPrefix() {
		return this.prefix;
	}
	private String prefix;
}
