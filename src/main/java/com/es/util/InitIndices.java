package com.es.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import com.es.client.ClientManager;

class InitIndices {
	private static final Logger log = Logger.getLogger(InitIndices.class);
	private int numIndices = 128;

	public void init() {
		Client client = ClientManager.getClient();
		int index = 0;
		for(; index<numIndices; index++) {
			log.info("index=" + index);
			client.admin().indices()
				.prepareCreate("test-index-"+index)
				.execute().actionGet();
			// this will block until clusterhealth is yellow.
			ClusterHealthResponse res = client.admin().cluster().prepareHealth()
				.setWaitForYellowStatus()
				.execute().actionGet();
			log.info("health="+res.getStatus().toString());
		}
	}
	
	public InitIndices(int i) {
		this.numIndices = i;
	}
	
	public static void main(String[] args) {
		if (args.length > 0) {
			try {
				InitIndices initInstance = new InitIndices(Integer.parseInt(args[0]));
				initInstance.init();
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
