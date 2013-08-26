package com.es.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.es.client.ClientManager;
import com.es.processor.ExecutionPolicy;

import static com.es.rax.RaxLocator.*;


public class ClientImpl implements ClientIFace {
	private static final Logger log = Logger.getLogger(ClientImpl.class);
	private static Client client = ClientManager.getClient();
	// currently not that useful at the moment.
	private static final String ES_TYPE = "metrics";

	@Override
	public Future<Boolean> insert(final String tenantId, final InsertRequest request) {
		return ExecutionPolicy.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				XContentBuilder content;
				try {
					content = createSourceContent(tenantId, request);
				} catch (IOException ie) {
					return false;
				}
				IndexResponse indexRes = client.prepareIndex(getIndex(tenantId), ES_TYPE)
						//.setId(getId(content))
						.setId(getId(tenantId, request))
						.setRouting(getRouting(tenantId))
						.setSource(content)
						//.setVersion(1)
						//.setVersionType(VersionType.EXTERNAL)
						.execute()
						.actionGet();
				log.debug("index=" + indexRes.getIndex() + " id=" + indexRes.getId() + " version=" + indexRes.getVersion());
				return true;
			}

			private XContentBuilder createSourceContent(String tenantId, InsertRequest request) throws IOException {
				XContentBuilder json = XContentFactory.jsonBuilder().startObject();

				json = json.field(TENANT_ID.toString(), tenantId);
				json = json.field(LOCATOR.toString(), request.getLocator());

				for (Map.Entry<String, Object> entry : request.getAnnotation().entrySet()) {
					json = json.field(entry.getKey(), entry.getValue());
				}
				json = json.endObject();
				return json;
			}
		});
	}

	@Override
	public Future<List<String>> getAllMetrics(final String tenantId, final SearchRequest query) {
		return ExecutionPolicy.submit(new Callable<List<String>>() {
			@Override
			public List<String> call() throws Exception {
				List<String> matched = new ArrayList<String>();
				SearchRequestBuilder request = createSearchRequest(tenantId, query);
				SearchResponse searchRes = request.execute().actionGet();
				for (SearchHit hit : searchRes.getHits().getHits()) {
					log.debug("id=" + hit.getId() + ", shard=" + hit.getShard() + ", version=" + hit.version());
					Map<String, Object> result = hit.getSource();
					matched.add((String) result.get(LOCATOR.toString()));
				}
				return matched;
			}

			private SearchRequestBuilder createSearchRequest(String tenantId, SearchRequest query) {
				SearchRequestBuilder request = client.prepareSearch(getIndex(tenantId))
						.setSize(500)
						.setRouting(getRouting(tenantId))
						.setVersion(true)
						.setQuery(QueryBuilders.fieldQuery(TENANT_ID.toString(), tenantId).analyzeWildcard(true));
				request = request.setQuery(QueryBuilders.fieldQuery(LOCATOR.toString(), query.getLocatorQuery())
						.analyzeWildcard(true));
				for (Map.Entry<String, Object> entry : query.getAnnotationQuery().entrySet()) {
					request = request.setQuery(QueryBuilders.fieldQuery(entry.getKey(), entry.getValue()).analyzeWildcard(true));
				}
				return request;
			}
		});
	}

	private String getIndex(String tenantId) {
		return "test-index-" + String.valueOf(Math.abs(tenantId.hashCode() % 128));
	}

	/** All requests from the same tenant should go to the same shard.
	 * @param tenantId
	 * @return 
	 */
	private String getRouting(String tenantId) {
		return tenantId;
	}

	private String getId(String tenantId, InsertRequest request) {
		return tenantId + request.toString();
	}



}
