package com.es.api;

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

import com.es.client.ClientManager;
import com.es.rax.RaxLocator;
import com.es.util.ClearIndexWorker;

import static com.es.rax.RaxLocator.*;


public class RoutingBenchmarkTest {

	private static final Logger log = Logger.getLogger(RoutingBenchmarkTest.class);
	static int numDocs = 100;
	static ClientIFace handler = null;
	private static final String tenantId = TENANT_ID.getPrefix() + "Asdfqwer";
	private static final String index = "test-index-55"; //matched the tenantId above.
	private static final String routing = "2"; //matched the tenantId above.

	@BeforeClass
	public static void generateData() throws InterruptedException {
		handler = new ClientImpl();
		log.info("benchmarking numDocs="+RoutingBenchmarkTest.numDocs);
		for (int i=0; i<numDocs; i++) {
			handler.insert(tenantId, RaxLocator.generateRaxLocatordata(
					UUID.randomUUID().toString(), 
					UUID.randomUUID().toString(), 
					UUID.randomUUID().toString()));
		}
		//sleep to let ES catch up.
		TimeUnit.SECONDS.sleep(10);
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
							QueryBuilders.queryString(TENANT_ID.toString() + ":" + TENANT_ID.getPrefix() + "*")
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
					.setRouting(tenantId)
					.setQuery(
							QueryBuilders.queryString(TENANT_ID.toString() + ":" + TENANT_ID.getPrefix() + "*")
							.analyzeWildcard(true))
					.execute().actionGet();
			log.debug("case:" + i + " found:" + searchRes.getHits().getTotalHits());
			Assert.assertTrue("search result empty", searchRes.getHits().getTotalHits() > 0);
		}
		log.info("with routing time="+stopWatch.stop().lastTaskTime().getMillis());
	}
	
}
