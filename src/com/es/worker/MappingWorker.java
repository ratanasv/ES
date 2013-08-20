package com.es.worker;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.es.client.ElasticClient;

final class MappingWorker {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		final XContentBuilder content = XContentFactory.jsonBuilder()
			.startObject()
				.startObject("metrics")
					.startObject("properties")
						.startObject("accountId")
							.field("type", "string")
						.endObject()
						.startObject("entityId")
							.field("type", "string")
						.endObject()
						.startObject("checkId")
							.field("type", "string")
						.endObject()
						.startObject("metric")
							.field("type", "string")
						.endObject()
						.startObject("unusedField")
							.field("type", "string")
						.endObject()
					.endObject()
				.endObject()
			.endObject();
		
		DeleteMappingResponse delRes =  ElasticClient.getClient().admin().indices().prepareDeleteMapping().setType("metrics")
			.execute().actionGet();
		//PutMappingResponse mapRes = ElasticClient.getClient().admin().indices().preparePutMapping("test-index-0")
		//	.setType("metrics").setSource(content).execute().actionGet();
		
		
	}

}
