package com.es.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.junit.Assert;

import com.es.client.ClientManager;

public class DeleteIndexWorker implements Runnable {
	private static final Logger log = Logger.getLogger(DeleteIndexWorker.class);
	private String indexToDelete;
	@Override
	public void run() {
		DeleteIndexResponse deleteRes;
		if (indexToDelete.equals("all")) {
			deleteRes = ClientManager.getClient().admin().indices().prepareDelete().execute().actionGet();
		} else {
			deleteRes = ClientManager.getClient().admin().indices().prepareDelete(indexToDelete).execute().actionGet();
		}
		log.info(indexToDelete + " deleted, health=" + ClientManager.getClient()
				.admin().cluster().prepareHealth().execute().actionGet().getStatus().toString());
		
	}
	
	DeleteIndexWorker(String s) {
		this.indexToDelete = s;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			log.info("nothing to do");
			System.exit(1);
		}
		
		ExecutorService exec = Executors.newCachedThreadPool();
		for (String s: args) {
			log.info("deleting " + s);
			exec.execute(new DeleteIndexWorker(s));
		}
		exec.shutdown();
		try {
			exec.awaitTermination(7, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			Assert.fail("waiting for threads failed");
		}
	}

}
