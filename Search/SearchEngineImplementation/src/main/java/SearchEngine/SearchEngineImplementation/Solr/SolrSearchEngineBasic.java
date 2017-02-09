package SearchEngine.SearchEngineImplementation.Solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class SolrSearchEngineBasic implements SearchEngineBasicInterface{
	
	protected CloudSolrClient solrClient = null;
	
	public SolrSearchEngineBasic(Map<String, Object> serverParams) {
		
		try {
			
			// 读取Zookeeper的配置
			String zkHost = serverParams.get("zkHost").toString();
			// 读取Solr Collection文档的配置
			String collection = serverParams.get("collection").toString();
			
			// 根据上述配置，初始化CloudSolrClient
			solrClient = new CloudSolrClient.Builder().withZkHost(zkHost).build();
			solrClient.setDefaultCollection(collection);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
			
	}
	
	public void cleanup() {
		
		// 关闭CloudSolrClient的连接
		if (solrClient != null) {
			try {
				solrClient.close();
				solrClient = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				solrClient = null;
			}
		}
		
	}
	
	
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		UpdateResponse response = null;
		
		try {
		
			// 适配部分：根据输入的统一文档ListingDocument，生成并添加Solr所使用的SolrInputDocument
			for (ListingDocument ld : documents) {
	
				SolrInputDocument sid = new SolrInputDocument();
				sid.addField("listing_id", ld.getListing_id());
				sid.addField("listing_title", ld.getListing_title());
				sid.addField("category_id", ld.getCategory_id());
				sid.addField("category_name", ld.getCategory_name());
				
				solrClient.add(sid);
			}
			
			// 写入Solr Cloud的索引
			response = solrClient.commit();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return response.toString();
	}
	
	public String query(Map<String, Object> queryParams) {
		// TODO Auto-generated method stub

		QueryResponse  response = null;
		
		try {
			
			// 适配部分：根据输入的搜索请求，生成Solr所能识别的查询
			String query = queryParams.get("query").toString();
			String[] terms = query.split("\\s+");
			StringBuffer sbQuery = new StringBuffer();
			for (String term : terms) {
				if (sbQuery.length() == 0) {
					sbQuery.append(term);
				} else {
					// 为了确保相关性，使用了AND的布尔操作
					sbQuery.append(" AND ").append(term);
				}
			}
			String[] fields = (String []) queryParams.get("fields");
			StringBuffer sbQf = new StringBuffer();
			for (String field : fields) {
				if (sbQf.length() == 0) {
					sbQf.append(field);
				} else {
					// 在多个字段上查询
					sbQf.append(" ").append(field);
				}
			}
			
			// 为支持翻页（pagination）操作的起始位置和返回结果数
			int start = (int)(queryParams.get("start"));
			int rows = (int)(queryParams.get("rows"));
			
			// 构建Solr使用的查询
			SolrQuery sq = new SolrQuery();
			sq.setParam("defType", "edismax");
			sq.set("q", sbQuery.toString());
			sq.set("qf", sbQf.toString());
			sq.set("start", start);
			sq.set("rows", rows);
			
			// 获取查询结果
			response = solrClient.query(sq);
//			SolrDocumentList list = response.getResults();
//			。。。这里略去后续统一文档拼装的实现。。。
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
		
		return response.toString();
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		// 测试索引接口
		Map<String, Object> serverParams = new HashMap<>();
		// 连接Solr Cloud的Zookeeper设置，根据你的需要设置
		serverParams.put("zkHost",  "192.168.1.48:9983");
		// 索引写入哪个Collection，根据你的需要设置。这里写入另一个测试的collection
		serverParams.put("collection",  "listing_collection_bySolrJ");
		// 初始化
		SolrSearchEngineBasic sse = new SolrSearchEngineBasic(serverParams);
		
		Map<String, Object> indexParams = new HashMap<>();
		ListingDocument ld1 = new ListingDocument(
				1001, "SolrJ索引测试标题1", 100001, "SolrJ索引测试类目1");
		ListingDocument ld2 = new ListingDocument(
				1002, "SolrJ索引测试标题2", 100002, "SolrJ索引测试类目2");
		List<ListingDocument> documents = new ArrayList<>();
		documents.add(ld1);
		documents.add(ld2);
		// 索引测试文档
		System.out.println(sse.index(documents, indexParams));
		
		sse.cleanup();
		sse = null;
		
		

		// 测试查询接口
		// 连接Solr Cloud的Zookeeper设置，根据你的需要设置
		serverParams.put("zkHost",  "192.168.1.48:9983");
		// 查询读取哪个Collection，根据你的需要设置。
		serverParams.put("collection",  "listing_collection");
		sse = new SolrSearchEngineBasic(serverParams);
		
		Map<String, Object> queryParams = new HashMap<>();
		// 查询关键词
		queryParams.put("query", "西红柿 方便面");
		// 在两个字段上查询
		queryParams.put("fields", 
				new String[] {"listing_title", "category_name"});
		queryParams.put("start", 0);	// 从第1条结果记录开始
		queryParams.put("rows", 5);		// 返回5条结果记录
		System.out.println(sse.query(queryParams));	// 查询并输出
		
		sse.cleanup();
		
		
	}

}
