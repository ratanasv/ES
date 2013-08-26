package com.es.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.es.client.ClientManager;
import com.es.processor.ExecutionPolicy;
import com.es.util.ClearIndexWorker;


public class IndexingTest {
	private static final Logger log = Logger.getLogger(IndexingTest.class);
	private static final int NUM_DOCS = 100;
	private static final ClientIFace HANDLER = new ClientImpl();
	private static final String ARBITRARY_TENANT = "ratanasv";
	
	@Before
	public void setup() {
		ClearIndexWorker.clear("all");
		for (int i=0; i<NUM_DOCS; i++) {
			InsertRequest req = new InsertRequest.Builder("a0.b" + i + ".c" + i + ".e" + i).build();
			HANDLER.insert(ARBITRARY_TENANT, req);
		}
		ExecutionPolicy.blockUntilNoTasksLeft();
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@Test
	public void testThatIndexingOccurs() throws InterruptedException, ExecutionException {
		for (int i=0; i<NUM_DOCS; i++) {
			SearchRequest query = new SearchRequest.Builder().locatorQuery("a0.*").build();
			Future<List<String>> result = HANDLER.getAllLocators(ARBITRARY_TENANT, query);
			Assert.assertEquals(NUM_DOCS, result.get().size());
			log.debug(result.toString());
		}
	}
	
	
	@After
	public void cleanup() {
		ClearIndexWorker.clear("all");
		CountResponse countRes = ClientManager.getClient().prepareCount().execute().actionGet();
		Assert.assertEquals(countRes.getCount(), 0);
	}

}
