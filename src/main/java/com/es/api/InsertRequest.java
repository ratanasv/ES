package com.es.api;

import java.util.HashMap;
import java.util.Map;

public class InsertRequest {

	public static class Builder {
		private Map<String, Object> annotation = new HashMap<String, Object>();
		private final String locator;
		
		public Builder(String locator) {
			this.locator = locator;
		}
		
		public InsertRequest build() {
			return new InsertRequest(this);
		}
		
		public Builder withAnnotation(Map<String, Object> annotation) {
			this.annotation = annotation;
			return this;
		}
	}

	private final String locator;
	private final Map<String, Object> annotation;



	public InsertRequest(Builder builder) {
		this.locator = builder.locator;
		this.annotation = builder.annotation;
	}

	public Map<String, Object> getAnnotation() {
		return annotation;
	}
	
	public String getLocator() {
		return locator;
	}
	

	@Override
	public String toString() {
		return "InsertRequest [locator=" + locator + ", annotation="
				+ annotation + "]";
	}
}
