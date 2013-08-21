package com.es.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.*;

import com.es.api.IOHandler;
import com.es.api.IOIface;

import static com.es.type.RaxLocator.*;

public final class IngestWorker implements Runnable{

	private static final Logger log = Logger.getLogger(IngestWorker.class);
	int numDocs = 100;

	@Override
	public void run() {
		double sum = 0.0;
		int failures = 0;
		for (int count=0; count<numDocs; count++) {
			IOIface handler = new IOHandler();
			Map<String, String> map = new HashMap<String, String>();
			map.put(ENTITY_ID.toString(), ENTITY_ID.getPrefix() + UUID.randomUUID());
			map.put(CHECK_ID.toString(), CHECK_ID.getPrefix() + UUID.randomUUID());
			map.put(METRIC.toString(), METRIC.getPrefix() + UUID.randomUUID());
			final String tenantId = TENANT_ID.getPrefix() + UUID.randomUUID();
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
		log.info("numThreads="+numThreads);
		ExecutorService exec = Executors.newCachedThreadPool();
		for (int i=0; i<numThreads; i++) {
			exec.execute(IngestWorker.IngestWorkerBuilder().numDocs(numDocs));
		}
		exec.shutdown();
	}

}
