package com.es.worker;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.apache.log4j.*;

import com.es.client.ElasticClient;


public class SearchWorker {
	private static final Logger log = Logger.getLogger(SearchWorker.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SearchResponse res = ElasticClient.getClient().prepareSearch("test-index-0")
	    		.setQuery(QueryBuilders.fieldQuery("lateToParty", "ltp-0"))//.analyzeWildcard(true))
	    		//.setQuery(QueryBuilders.fieldQuery("entityId", "en*").analyzeWildcard(true))
	    		//.setQuery(QueryBuilders.fieldQuery("checkId", "ch*").analyzeWildcard(true))
	    		.execute()
	    		.actionGet();
		log.info(res.getHits().getTotalHits());
	}

}
