package com.es.processor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutionPolicy {
	private static int corePoolSize = 16;
	private static int maximumPoolSize = 16;
	private static long keepAliveTime = 2;
	private static TimeUnit unit = TimeUnit.MINUTES;
	private static BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
	private static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(
			corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	
	public static ExecutorService getExecutorService() {
		return EXECUTOR_SERVICE;
	}
	
}
