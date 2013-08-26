package com.es.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import com.es.client.ClientManager;
import com.es.processor.ExecutionPolicy;

public class InefficientMappingBenchmarkTest {
	private static final Logger log = Logger.getLogger(InefficientMappingBenchmarkTest.class);
	private static final String TENANT = "acAsdfqwer";
	private static final String index = "test-index-55"; //matched the tenantId above.
	private static final int ITER = 500;

	@Test
	public void manyFieldsMapping() throws IOException, InterruptedException, ExecutionException {

		clearMapping(index);
		ClientIFace handler = new ClientImpl();
		StopWatch watch = new StopWatch().start();
		for (int i=0; i<ITER; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("useless-"+i, String.valueOf(UUID.randomUUID()));
			InsertRequest req = new InsertRequest.Builder("a0.b0.c0.d0").withAnnotation(map).build();
			handler.insert(TENANT, req);
		}

		ExecutionPolicy.blockUntilNoTasksLeft();
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();


		for (int i=0; i<ITER; i++) {
			Map<String, Object> query = new HashMap<String, Object>();
			query.put("useless-"+i, "*");
			SearchRequest req = new SearchRequest.Builder().annotationQuery(query).build();
			Future<List<String>> result = handler.getAllLocators(TENANT, req);
			Assert.assertEquals(1, result.get().size());
		}
		log.info("many-field-mapping took " + watch.stop().lastTaskTime().getMillis());
		clearMapping(index);
	}

	@Test
	public void fewFieldsMapping() throws IOException, InterruptedException, ExecutionException {
		clearMapping(index);
		ClientIFace handler = new ClientImpl();
		StopWatch watch = new StopWatch().start();
		for (int i=0; i<ITER; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("useless-"+ (i%4), String.valueOf(UUID.randomUUID()));
			InsertRequest req = new InsertRequest.Builder("a0.b0.c0.d0").withAnnotation(map).build();
			handler.insert(TENANT, req);
		}

		ExecutionPolicy.blockUntilNoTasksLeft();
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();


		for (int i=0; i<ITER; i++) {
			Map<String, Object> query = new HashMap<String, Object>();
			query.put("useless-"+ (i%4), "*");
			SearchRequest req = new SearchRequest.Builder().annotationQuery(query).build();
			Future<List<String>> result = handler.getAllLocators(TENANT, req);
			Assert.assertEquals(ITER/4, result.get().size());
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
