package com.es.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import com.es.client.ElasticClient;

class InitIndicesWorker implements Runnable {
	private static final Logger log = Logger.getLogger(InitIndicesWorker.class);
	static int numIndices = 128;
	@Override
	public void run() {
		int index = 0;
		for(; index<numIndices; index++) {
			log.info("index=" + index);
			ElasticClient.getClient().admin().indices()
				.prepareCreate("test-index-"+index)
				.execute().actionGet();
			// this will block until clusterhealth is yellow.
			ClusterHealthResponse res = ElasticClient.getClient()
				.admin().cluster().prepareHealth().setWaitForYellowStatus()
				.execute().actionGet();
			log.info("health="+res.getStatus().toString());
		}
	}
	
	
	public static void main(String[] args) {
		if (args.length > 0) {
			try {
				InitIndicesWorker.numIndices = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i=0; i<1; i++) {
			exec.execute(new InitIndicesWorker());
		}
		exec.shutdown();
	}
}
