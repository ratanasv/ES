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
	
	//private static final ExecutorService insertExec = new ThreadPoolExecutor(
	//		arg0, arg1, arg2, arg3, arg4)

	@Override
	public Future<Boolean> insert(final String tenantId, final Map<String, String> map) {
		return ExecutionPolicy.getExecutorService().submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				XContentBuilder content;
				try {
					content = createSourceContent(tenantId, map);
				} catch (IOException ie) {
					return false;
				}
				IndexResponse response = client.prepareIndex(getIndex(tenantId), ES_TYPE)
						//.setId(getId(content))
						.setId(getId(tenantId, map))
						.setRouting(getRouting(tenantId))
						.setSource(content)
						//.setVersion(1)
						//.setVersionType(VersionType.EXTERNAL)
						.execute()
						.actionGet();
				return true;
			}
			
			private XContentBuilder createSourceContent(String tenantId, Map<String, String> map) throws IOException {
				XContentBuilder json = XContentFactory.jsonBuilder().startObject();
				// map might already contain tenantId already.
				if (!map.containsKey(TENANT_ID.toString())) {
					json = json.field(TENANT_ID.toString(), tenantId);
				}
				for (Map.Entry<String, String> entry : map.entrySet()) {
					json = json.field(entry.getKey(), entry.getValue());
				}
				json = json.endObject();
				return json;
			}
		});
	}

	@Override
	public Future<List<Map<String, Object>>> search(final String tenantId, final Map<String, String> query) {
		return ExecutionPolicy.getExecutorService().submit(new Callable<List<Map<String, Object>>>() {
			@Override
			public List<Map<String, Object>> call() throws Exception {
				List<Map<String, Object>> matched = new ArrayList<Map<String,Object>>();
		    	SearchRequestBuilder request = createSearchRequest(tenantId, query);
		    	SearchResponse searchRes = request.execute().actionGet();
		    	for (SearchHit hit : searchRes.getHits().getHits()) {
		    		log.debug("id=" + hit.getId() + ", shard=" + hit.getShard() + ", version=" + hit.version());
		    		matched.add(hit.getSource());
		    	}
		    	return matched;
			}
			
			private SearchRequestBuilder createSearchRequest(String tenantId, Map<String, String> map) {
				SearchRequestBuilder request = client.prepareSearch(getIndex(tenantId))
					.setSize(500)
					.setRouting(getRouting(tenantId))
					.setVersion(true)
					.setQuery(QueryBuilders.fieldQuery(TENANT_ID.toString(), tenantId).analyzeWildcard(true));
				for(Map.Entry<String, String> entry : map.entrySet()) {
					request = request.setQuery(QueryBuilders.fieldQuery(entry.getKey(), entry.getValue()).analyzeWildcard(true));
				}
				return request;
			}
		});
		
	}

	private String getIndex(String tenantId) {
		return "test-index-" + String.valueOf(Math.abs(tenantId.hashCode() % 128));
	}

	private String getRouting(String tenantId) {
		return tenantId;
	}
	
	
	/**
	 * WARNING: XContentBuilder does not implement equals and hashCode, so this method is obsolete.
	 * @param content
	 * @return
	 */
	private String getId(XContentBuilder content) {
		return String.valueOf(content.hashCode());
	}
	
	/** Return the hashCode which can be used as the "id" of the arguments. 
	 * This is implemented since XContentBuilder does not.
	 * @param tenantId
	 * @param map
	 * @return hashCode (or "id").
	 */
	private String getId(String tenantId, Map<String, String> map) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((map == null) ? 0 : map.hashCode());
		result = prime * result
				+ ((tenantId == null) ? 0 : tenantId.hashCode());
		return String.valueOf(result);
	}

}
