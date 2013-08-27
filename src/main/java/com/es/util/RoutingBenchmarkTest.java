package com.es.util;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.UUID;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.es.api.ClientIFace;
import com.es.api.ClientImpl;
import com.es.api.InsertRequest;
import com.es.api.InsertRequest.Builder;
import com.es.client.ClientManager;
import com.es.processor.ExecutionPolicy;


public class RoutingBenchmarkTest {

	private static final Logger log = Logger.getLogger(RoutingBenchmarkTest.class);
	static int numDocs = 40;
	static ClientIFace handler = null;
	private static final String TENANT = "acAsdfqwer";
	private static final String index = "test-index-55"; //matched the tenantId above.
	private static final String routing = "2"; //matched the tenantId above.

	@BeforeClass
	public static void generateData() throws InterruptedException {
		handler = new ClientImpl();
		log.info("benchmarking numDocs="+RoutingBenchmarkTest.numDocs);
		for (int i=0; i<numDocs; i++) {
			InsertRequest req = new InsertRequest.Builder("a0.b0.c0.d0.e0.f0").build();
			handler.insert(TENANT, req);
		}
		ExecutionPolicy.blockUntilNoTasksLeft();
	}

	@AfterClass
	public static void clearData() {
		ClearIndexWorker.clear(index);
	}

	@Test
	public void withoutRouting() {

		log.info("refresing indices");
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();
		ClusterHealthResponse healthRes = ClientManager.getClient()
				.admin().cluster().prepareHealth().setWaitForYellowStatus()
				.execute().actionGet();
		log.info("health="+healthRes.getStatus().toString());
		StopWatch stopWatch = new StopWatch().start();
		for (int i=0; i<numDocs; i++) {
			SearchResponse searchRes = ClientManager.getClient()
					.prepareSearch(index)
					.setQuery(
							QueryBuilders.queryString("LOCATOR" + ":" + "*")
							.analyzeWildcard(true))
							.execute().actionGet();
			log.debug("case:" + i + " found:" + searchRes.getHits().getTotalHits());
			Assert.assertTrue("search result empty", searchRes.getHits().getTotalHits() > 0);
		}
		log.info("w/o routing time="+stopWatch.stop().lastTaskTime().getMillis());
	}

	@Test
	public void withRouting() {
		log.info("refresing indices");
		ClientManager.getClient().admin().indices().prepareRefresh().execute().actionGet();
		ClusterHealthResponse healthRes = ClientManager.getClient()
				.admin().cluster().prepareHealth().setWaitForYellowStatus()
				.execute().actionGet();
		log.info("health="+healthRes.getStatus().toString());

		StopWatch stopWatch = new StopWatch().start();
		for (int i=0; i<numDocs; i++) {
			SearchResponse searchRes = ClientManager.getClient()
					.prepareSearch(index)
					.setRouting(TENANT)
					.setQuery(
							QueryBuilders.queryString("LOCATOR" + ":" + "*")
							.analyzeWildcard(true))
							.execute().actionGet();
			log.debug("case:" + i + " found:" + searchRes.getHits().getTotalHits());
			Assert.assertTrue("search result empty", searchRes.getHits().getTotalHits() > 0);
		}
		log.info("with routing time="+stopWatch.stop().lastTaskTime().getMillis());
	}

}
