package com.es.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.es.client.ClientManager;
import com.es.processor.ExecutionPolicy;
import com.es.util.ClearIndexWorker;
import com.es.worker.MappingWorker;


public class IndexingTest {
	private static final Logger log = Logger.getLogger(IndexingTest.class);
	private static final int NUM_DOCS = 100;
	private static final ClientIFace HANDLER = new ClientImpl();
	private static final String TENANT = "ratanasv";
	private static final String INDEX = "test-index-50";
	private static final String[] greekLetters = {"alpha", "beta", "delta", "gamma", "omega", };
	
	@BeforeClass
	public static void setup() throws IOException {
		ClearIndexWorker.clear("all");
		for (int i=0; i<NUM_DOCS; i++) {
			Map<String, Object> annotation = new HashMap<String, Object>();
			annotation.put("foo", "bar");
			annotation.put("long-val", i);
			InsertRequest req = new InsertRequest.Builder(bogusLocator(i))
				.withAnnotation(annotation)
				.build();
			HANDLER.insert(TENANT, req);
		}
		ExecutionPolicy.blockUntilNoTasksLeft();
		ClientManager.getClient().admin().indices().prepareRefresh(INDEX).execute().actionGet();
	}
	
	@Test
	public void testWildcard() throws InterruptedException, ExecutionException {
		SearchRequest query = new SearchRequest.Builder().locatorQuery("alpha*").build();
		Future<List<String>> result = HANDLER.getAllLocators(TENANT, query);
		log.debug(result.get().toString());
		Assert.assertEquals("trailing wildcard", NUM_DOCS, result.get().size());

		query = new SearchRequest.Builder().locatorQuery("alpha*omega0").build();
		result = HANDLER.getAllLocators(TENANT, query);
		log.debug(result.get().toString());
		Assert.assertEquals("wildcard in the middle", 1, result.get().size());
		
		
		query = new SearchRequest.Builder().locatorQuery("*omega0").build();
		result = HANDLER.getAllLocators(TENANT, query);
		log.debug(result.get().toString());
		Assert.assertEquals("wildcard in the front", 1, result.get().size());
		
		
	}
	
	@Test
	public void testAnnotation() throws InterruptedException, ExecutionException {
		Map<String, Object> annotation = new HashMap<String, Object>();
		annotation.put("foo", "bar");
		SearchRequest query = new SearchRequest.Builder().annotationQuery(annotation).build();
		Future<List<String>> result = HANDLER.getAllLocators(TENANT, query);
		Assert.assertEquals("common annotation", NUM_DOCS, result.get().size());
		
		annotation = new HashMap<String, Object>();
		annotation.put("long-val", "5");
		query = new SearchRequest.Builder().annotationQuery(annotation).build();
		result = HANDLER.getAllLocators(TENANT, query);
		Assert.assertEquals("unique annotation", 1, result.get().size());
	}
	
	@AfterClass
	public static void cleanup() throws IOException {
		ClearIndexWorker.clear("all");
		CountResponse countRes = ClientManager.getClient().prepareCount().execute().actionGet();
		Assert.assertEquals(countRes.getCount(), 0);
		
	}
	
	private static String bogusLocator(int i) {
		StringBuilder builder = new StringBuilder(greekLetters.length * 6);
		for (int j=0; j<greekLetters.length-1; j++) {
			builder.append(greekLetters[j]);
			//builder.append(i);
			builder.append(".");
		}
		builder.append(greekLetters[greekLetters.length-1]+ String.valueOf(i));
		return builder.toString();
	}

}
