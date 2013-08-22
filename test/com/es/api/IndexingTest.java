package com.es.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.es.client.ElasticClient;
import com.es.worker.IngestWorker;

import static com.es.type.RaxLocator.*;

public class IndexingTest {
	private static final Logger log = Logger.getLogger(IndexingTest.class);
	@Test
	public void testThatIndexingOccurs() {
		IOIface handler = new IOHandler();
		String tenantId = TENANT_ID.getPrefix() + "Asdfqwer";
		for (int i=0; i<20; i++) {
			Map<String, String> map = IngestWorker.generateRaxLocatordata(String.valueOf(i), String.valueOf(i), 
					String.valueOf(i));
			handler.insert(tenantId, map);
		}
		ElasticClient.getClient().admin().indices().prepareRefresh().execute().actionGet();
		for (int i=0; i<20; i++) {
			Map<String, String> query = new HashMap<String, String>();
			query.put(ENTITY_ID.toString(), ENTITY_ID.getPrefix()+String.valueOf(i));
			List<Map<String, Object>> result = handler.search(tenantId, query);
			Assert.assertEquals(1, result.size());
			log.debug(result.toString());
		}
		

	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Result result = JUnitCore.runClasses(IndexingTest.class);
		log.info("success: " + (result.getRunCount()-result.getFailureCount()) );
		log.info("failures: " + result.getFailureCount());
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
		}
	}

}
