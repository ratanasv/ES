package com.es.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface ClientIFace {

	public Future<Boolean> insert(String tenantId, InsertRequest request);
	
	public Future<List<String>> getAllLocators(String tenantId, SearchRequest query);
	
	
}
