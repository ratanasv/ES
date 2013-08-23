package com.es.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface ClientIFace {
	
	/**
	 * Insert key-value pairs with a specific tenantId into an ES cluster.
	 * @param tenantId self-explanatory.
	 * @param map key-value pairs.
	 * @return true if insert succeeds, false otherwise.
	 */
	public Future<Boolean> insert(String tenantId, Map<String, String> map);
	
	
	/** 
	 * Search for entries with a query. This will return a list of "sources", where each source is
	 * the key-value pairs you inserted earlier.
	 * @param tenantId self-explanatory.
	 * @param query key-value pairs where each value could contain the wildcard character (*). 
	 * @return A list of sources you inserted earlier.
	 */
	public Future<List<Map<String, Object>>> search(String tenantId, Map<String, String> query);
	
	
}
