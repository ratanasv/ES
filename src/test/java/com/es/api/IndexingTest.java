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
import com.es.rax.RaxLocator;
import com.es.util.ClearIndexWorker;

import static com.es.rax.RaxLocator.*;

public class IndexingTest {
	private static final Logger log = Logger.getLogger(IndexingTest.class);
	private static final int NUM_DOCS = 100;
	private static final ClientIFace HANDLER = new ClientImpl();
	private static final String ARBITRARY_TENANT_ID = TENANT_ID.getPrefix() + "Asdfqwer";
	
	@Before
	public void setup() {
		ClearIndexWorker.clear("all");
		for (int i=0; i<NUM_DOCS; i++) {
			Map<String, Object> map = RaxLocator.generateRaxLocatordata(String.valueOf(i), String.valueOf(i), 
					String.valueOf(i));
			HANDLER.insert(ARBITRARY_TENANT_ID, map);
		}
		ExecutionPolicy.blockUntilNoTasksLeft();
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@Test
	public void testThatIndexingOccurs() throws InterruptedException, ExecutionException {
		for (int i=0; i<NUM_DOCS; i++) {
			Map<String, String> query = new HashMap<String, String>();
			query.put(ENTITY_ID.toString(), ENTITY_ID.getPrefix()+String.valueOf(i));
			Future<List<Map<String, Object>>> result = HANDLER.search(ARBITRARY_TENANT_ID, query);
			Assert.assertEquals(1, result.get().size());
			Map<String, Object> map = RaxLocator.generateRaxLocatordata(String.valueOf(i), String.valueOf(i), 
					String.valueOf(i));
			Assert.assertEquals(map, result.get().get(0));
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
