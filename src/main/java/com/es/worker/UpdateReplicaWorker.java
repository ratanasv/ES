package com.es.worker;

import org.elasticsearch.common.settings.ImmutableSettings;

import com.es.client.ClientManager;

final class UpdateReplicaWorker {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ClientManager.getClient().admin().indices().prepareUpdateSettings().setSettings(
			ImmutableSettings.builder().put("index.number_of_replicas",2))
			.execute().actionGet();
	}

}
