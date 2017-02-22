package Recommendation.RecommendationImplementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import SearchEngine.SearchEngineImplementation.Elasticsearch.ElasticSearchEngineBasic;

public class ElasticsearchItemBasedCF {
	
	ElasticSearchEngineBasic ese = null;
	ObjectMapper mapper = null;
	
	public ElasticsearchItemBasedCF(Map<String, Object> serverParams) {
		ese = new ElasticSearchEngineBasic(serverParams);
		mapper = new ObjectMapper();
	}
	
	// 通过给定的商品ID和基于物品的协同过滤，进行推荐
	public List<Long> recommend(Long listingId) {
		
		List<Long> listingIds = new ArrayList<>();
		
		try {
			Map<String, Object> queryParams = new HashMap<>();
			
			// 和Solr有所不同，需要在这里指定索引和类型
			queryParams.put("index", "listing_vs_user");
			queryParams.put("type", "listing");
			
			// 根据商品ID，查询哪些用户购买过该商品
			queryParams.put("query", listingId);
			queryParams.put("fields", new String[] {"listing_id"});
			queryParams.put("from", 0);		// 从第1条结果记录开始
			queryParams.put("size", 1);
			queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
			JsonNode jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			Iterator<JsonNode> iter = jnDocs.iterator();
			String users = "";
			if (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				users = jnDoc.get("_source").get("purchased_users").asText();
				System.out.println(
						String.format("给定的商品是：%s", jnDoc.get("_source").get("listing_title").asText())
						);
			}
			
			System.out.println(String.format("基于物品的协同过滤-推荐商品是："));
					
			// 根据购买者列表构建查询，进行基于商品的协同过滤
			queryParams.clear();
			
			queryParams.put("index", "listing_vs_user");
			queryParams.put("type", "listing");
			
			queryParams.put("query", users);
			queryParams.put("fields", new String[] {"purchased_users"});
			queryParams.put("from", 0);		// 从第1条结果记录开始
			queryParams.put("size", 11);	// 返回前10条结果记录（为了排除输入的商品自己，取11个结果）
			queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
			
			// 获取排名靠前的商品 
			jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			iter = jnDocs.iterator();
			while (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				// 由于搜索引擎中包含输入商品本身，排除它自己。也可以修改ElasticSearchEngineBasic的实现，使用filter来排除
				Long listingIdRecom = jnDoc.get("_source").get("listing_id").asLong();
				if (listingIdRecom == listingId) continue;
				
				listingIds.add(listingIdRecom);
				System.out.println(
						String.format("\t%s", jnDoc.get("_source").get("listing_title").asText())
						);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return listingIds;
		}
		
		// 返回推荐商品的列表
		return listingIds;
		
	}
	
	public void cleanup() {
		ese.cleanup();
	}
	
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		// Elasticsearch服务器的设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
		serverParams.put("server",  new byte[]{(byte)192,(byte)168,1,48});
		serverParams.put("port", 9300);
		serverParams.put("cluster", "ECommerce");
		
		ElasticsearchItemBasedCF eicf = new ElasticsearchItemBasedCF(serverParams);
		
		while (true) {
			BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
            System.out.print("请输入商品ID：");  
            String content = strin.readLine();
            
            if ("exit".equalsIgnoreCase(content)) break;
			eicf.recommend(Long.parseLong(content));
		}
		
		eicf.cleanup();
		

	}

}
