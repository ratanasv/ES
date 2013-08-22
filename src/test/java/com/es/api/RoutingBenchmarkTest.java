package com.es.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.es.client.ElasticClient;
import com.es.worker.ClearIndexWorker;

import static com.es.rax.RaxLocator.*;


public class RoutingBenchmarkTest {
	
	private static final Logger log = Logger.getLogger(RoutingBenchmarkTest.class);
	static int numDocs = 100;
	static int numThreads = 10;
	static IOIface handler = null;
	
	@Before
	public void generateData() {
		handler = new IOHandler();
		
		log.info("benchmarking numDocs="+RoutingBenchmarkTest.numDocs
			+" numThreads=" + RoutingBenchmarkTest.numThreads);
		IngestWorker.Ingest(numThreads, numDocs);
	}
	
	@After
	public void clearData() {
		ClearIndexWorker.clear("all");
	}
	
	@Test
	public void withoutRouting() {

		log.info("refresing indices");
		ElasticClient.getClient().admin().indices().prepareRefresh().execute().actionGet();
		ClusterHealthResponse healthRes = ElasticClient.getClient()
				.admin().cluster().prepareHealth().setWaitForYellowStatus()
				.execute().actionGet();
		log.info("health="+healthRes.getStatus().toString());
		StopWatch stopWatch = new StopWatch().start();
		for (int i=0; i<numDocs; i++) {
			
			SearchResponse searchRes = ElasticClient.getClient().prepareSearch("test-index-0").setQuery(
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
		ElasticClient.getClient().admin().indices().prepareRefresh().execute().actionGet();
		ClusterHealthResponse healthRes = ElasticClient.getClient()
				.admin().cluster().prepareHealth().setWaitForYellowStatus()
				.execute().actionGet();
		log.info("health="+healthRes.getStatus().toString());
		
		StopWatch stopWatch = new StopWatch().start();
		for (int i=0; i<numDocs; i++) {
			SearchResponse searchRes = ElasticClient.getClient().prepareSearch("test-index-0").setRouting("0").setQuery(
					QueryBuilders.queryString(TENANT_ID.toString() + ":" + TENANT_ID.getPrefix() + "*")
					.analyzeWildcard(true))
					.execute().actionGet();
			log.debug("case:" + i + " found:" + searchRes.getHits().getTotalHits());
			Assert.assertTrue("search result empty", searchRes.getHits().getTotalHits() > 0);
		}
		log.info("with routing time="+stopWatch.stop().lastTaskTime().getMillis());
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Result result = JUnitCore.runClasses(RoutingBenchmarkTest.class);
		log.info("success: " + (result.getRunCount()-result.getFailureCount()) );
		log.info("failures: " + result.getFailureCount());
		for (Failure failure : result.getFailures()) {
		      System.out.println(failure.toString());
		}
	}

	

}
