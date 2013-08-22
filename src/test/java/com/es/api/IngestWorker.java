package com.es.api;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.*;
import org.junit.Assert;

import com.es.api.IOHandler;
import com.es.api.IOIface;
import com.es.rax.RaxLocator;

import static com.es.rax.RaxLocator.*;

public final class IngestWorker implements Runnable{

	private static final Logger log = Logger.getLogger(IngestWorker.class);
	private int numDocs = 100;
	private int numThreads = 1;
	private String tenantId = null;

	@Override
	public void run() {
		double sum = 0.0;
		int failures = 0;
		for (int count=0; count<numDocs; count++) {
			IOIface handler = new IOHandler();
			Map<String, String> map = RaxLocator.generateRaxLocatordata(UUID.randomUUID().toString(), 
					UUID.randomUUID().toString(), UUID.randomUUID().toString());

			if (this.tenantId == null) {
				this.tenantId = TENANT_ID.getPrefix() + UUID.randomUUID();
			}

			long start = System.currentTimeMillis();
			boolean ok = handler.insert(tenantId, map);
			if (!ok) {
				failures++;
			}
			long stop = System.currentTimeMillis();
			sum += (stop-start);

			if (count % 10 == 0 && count != 0) {
				log.info("Average index latency (ms):" + sum/(count-failures));
				log.info("Failures : " + failures);
			}
		}
	}

	public static IngestWorker IngestWorkerBuilder() {
		return new IngestWorker();
	}

	public IngestWorker numDocs(int i) {
		this.numDocs = i;
		return this;
	}

	public IngestWorker tenantId(String i) {
		this.tenantId = i;
		return this;
	}

	public static void Ingest(int numThreads, int numDocs) {
		log.info("numThreads=" + numThreads + " numDocs=" + numDocs);
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i=0; i<numThreads; i++) {
			exec.execute(IngestWorker.IngestWorkerBuilder().numDocs(numDocs));
		}
		exec.shutdown();
		try {
			exec.awaitTermination(7, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			Assert.fail("waiting for threads failed");
		}
	}
	
	public static void Ingest(String tenantId, int numThreads, int numDocs) {
		log.info("numThreads=" + numThreads + " numDocs=" + numDocs);
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i=0; i<numThreads; i++) {
			exec.execute(IngestWorker.IngestWorkerBuilder().numDocs(numDocs).tenantId(tenantId));
		}
		exec.shutdown();
		try {
			exec.awaitTermination(7, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			Assert.fail("waiting for threads failed");
		}
	}

	public static void main(String[] args) {
		int numThreads = 1;
		int numDocs = 100;
		if (args.length > 0) {
			try {
				numThreads = Integer.parseInt(args[0]);
				if (args.length > 1) {
					numDocs = Integer.parseInt(args[1]);
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		IngestWorker.Ingest(numThreads, numDocs);
	}

}
