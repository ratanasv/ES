package com.es.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;

import com.es.client.ClientManager;

public class ClearIndexWorker implements Runnable {
	private static final Logger log = Logger.getLogger(ClearIndexWorker.class);
	private String indexToClear;
	@Override
	public void run() {
		CountResponse countRes;
		if (indexToClear.equals("all")) {
			countRes = ClientManager.getClient().prepareCount().execute().actionGet();
		} else {
			countRes = ClientManager.getClient().prepareCount(indexToClear).execute().actionGet();
		}
		log.info(indexToClear + ", numDocs=" + countRes.getCount());
		if (indexToClear.equals("all")) {
			ClientManager.getClient().prepareDeleteByQuery()
			.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		} else {
			ClientManager.getClient().prepareDeleteByQuery(indexToClear)
				.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		}
		log.info(indexToClear + " cleared, health=" + ClientManager.getClient()
				.admin().cluster().prepareHealth().execute().actionGet().getStatus().toString());
	}
	
	public ClearIndexWorker(String s) {
		this.indexToClear = s;
	}
	
	public static void clear(String ... args) {
		ExecutorService exec = Executors.newCachedThreadPool();
		for (String s: args) {
			log.info("clearing " + s);
			exec.execute(new ClearIndexWorker(s));
		}
		exec.shutdown();
		try {
			exec.awaitTermination(7, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			Assert.fail("waiting for threads failed");
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ClearIndexWorker.clear(args);
	}

	

}
