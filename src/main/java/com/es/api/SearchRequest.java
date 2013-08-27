package com.es.api;

import java.util.HashMap;
import java.util.Map;

public class SearchRequest {


	public static class Builder {
		private String locatorQuery = "";
		private Map<String, Object> annotationQuery = new HashMap<String, Object>();
		
		public Builder locatorQuery(String query) {
			this.locatorQuery = query;
			return this;
		}
		
		public Builder annotationQuery(Map<String, Object> query) {
			this.annotationQuery = query;
			return this;
		}
		
		public SearchRequest build() {
			return new SearchRequest(this);
		}
	}
	
	private final String locatorQuery;
	private final Map<String, Object> annotationQuery;
	
	public SearchRequest(Builder builder) {
		this.locatorQuery = builder.locatorQuery;
		this.annotationQuery = builder.annotationQuery;
	}
	
	public String getLocatorQuery() {
		return locatorQuery;
	}

	public Map<String, Object> getAnnotationQuery() {
		return annotationQuery;
	}
	
	@Override
	public String toString() {
		return "SearchRequest [locatorQuery=" + locatorQuery
				+ ", annotationQuery=" + annotationQuery + "]";
	}

}
