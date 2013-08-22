package com.es.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.es.client.ClientManager;
import com.es.rax.RaxLocator;
import com.es.worker.ClearIndexWorker;

import static com.es.rax.RaxLocator.*;

public class IndexingTest {
	private static final Logger log = Logger.getLogger(IndexingTest.class);
	@Test
	public void testThatIndexingOccurs() {
		ClientIFace handler = new ClientImpl();
		ClearIndexWorker.clear("all");
		String tenantId = TENANT_ID.getPrefix() + "Asdfqwer";
		for (int i=0; i<20; i++) {
			Map<String, String> map = RaxLocator.generateRaxLocatordata(String.valueOf(i), String.valueOf(i), 
					String.valueOf(i));
			handler.insert(tenantId, map);
		}
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();
		for (int i=0; i<20; i++) {
			Map<String, String> query = new HashMap<String, String>();
			query.put(ENTITY_ID.toString(), ENTITY_ID.getPrefix()+String.valueOf(i));
			List<Map<String, Object>> result = handler.search(tenantId, query);
			Assert.assertEquals(1, result.size());
			log.debug(result.toString());
		}
		ClearIndexWorker.clear("all");
		CountResponse countRes = ClientManager.getClient().prepareCount().execute().actionGet();
		Assert.assertEquals(countRes.getCount(), 0);
	}

}
