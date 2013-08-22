package com.es.api;

import static com.es.rax.RaxLocator.TENANT_ID;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import com.es.client.ClientManager;
import com.es.worker.ClearIndexWorker;

public class InefficientMappingBenchmarkTest {
	private static final Logger log = Logger.getLogger(InefficientMappingBenchmarkTest.class);
	private static final String tenantId = TENANT_ID.getPrefix() + "Asdfqwer";
	private static final String index = "test-index-55"; //matched the tenantId above.

	@Test
	public void manyFieldsMapping() throws IOException {
		final int iter = 1000;
		clearMapping(index);
		ClientIFace handler = new ClientImpl();
		for (int i=0; i<iter; i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("useless-"+i, String.valueOf(UUID.randomUUID()));
			handler.insert(tenantId, map);
		}

		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();

		StopWatch watch = new StopWatch().start();
		for (int i=0; i<iter; i++) {
			Map<String, String> query = new HashMap<String, String>();
			query.put("useless-"+i, "*");
			List<Map<String, Object>> result = handler.search(tenantId, query);
			Assert.assertEquals(iter/4, result.size());
		}
		log.info("many-field-mapping took " + watch.stop().lastTaskTime().getMillis());
		clearMapping(index);
	}
	
	@Test
	public void fewFieldsMapping() throws IOException {
		clearMapping(index);
		ClientIFace handler = new ClientImpl();
		for (int i=0; i<1000; i++) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("useless-"+ (i%4), String.valueOf(UUID.randomUUID()));
			handler.insert(tenantId, map);
		}

		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();

		StopWatch watch = new StopWatch().start();
		for (int i=0; i<1000; i++) {
			Map<String, String> query = new HashMap<String, String>();
			query.put("useless-"+ (i%4), "*");
			List<Map<String, Object>> result = handler.search(tenantId, query);
			Assert.assertEquals(1, result.size());
		}
		log.info("few-field-mapping took " + watch.stop().lastTaskTime().getMillis());
		clearMapping(index);
	}

	public static void clearMapping(String index) throws IOException {
		final XContentBuilder content = XContentFactory.jsonBuilder()
				.startObject()
					.startObject("metrics")
						.startObject("properties")
						.endObject()
					.endObject()
				.endObject();
		PutMappingResponse mapRes = ClientManager.getClient().admin().indices().preparePutMapping(index)
				.setType("metrics").setSource(content).execute().actionGet();
		DeleteMappingResponse delRes =  ClientManager.getClient().admin().indices()
				.prepareDeleteMapping(index).setType("metrics")
				.execute().actionGet();
	}
}
