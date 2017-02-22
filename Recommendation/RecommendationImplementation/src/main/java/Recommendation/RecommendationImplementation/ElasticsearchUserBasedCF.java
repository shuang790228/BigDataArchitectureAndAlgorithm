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

public class ElasticsearchUserBasedCF {
	
	ElasticSearchEngineBasic ese = null;
	ObjectMapper mapper = null;
	
	public ElasticsearchUserBasedCF(Map<String, Object> serverParams) {
		ese = new ElasticSearchEngineBasic(serverParams);
		mapper = new ObjectMapper();
	}
	
	// 通过给定的用户ID和基于用户的协同过滤，进行推荐
	public List<Long> recommend(Long userId) {
		
		List<Long> listingIds = new ArrayList<>();
		StringBuffer users = new StringBuffer();
		
		// 根据给定用户的购买记录，查找最近邻的用户
		try {
			Map<String, Object> queryParams = new HashMap<>();
			
			// 根据用户ID，查询该用户购买过哪些商品
			queryParams.put("index", "user_vs_listing");
			queryParams.put("type", "user");
			
			queryParams.put("query", userId);
			queryParams.put("fields", new String[] {"user_id"});
			queryParams.put("from", 0);		// 从第1条结果记录开始
			queryParams.put("size", 1);
			queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
			JsonNode jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			Iterator<JsonNode> iter = jnDocs.iterator();
			String purchasedListing = "";
			if (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				purchasedListing = jnDoc.get("_source").get("purchased_listing").asText();
				System.out.println(
						String.format("该用户购买过的商品是：%s...", purchasedListing.substring(0, 50))
						);
			}
			
			
			// 根据购买过的商品，查找最近邻的用户
			queryParams.clear();
			
			queryParams.put("index", "user_vs_listing");
			queryParams.put("type", "user");
			
			queryParams.put("query", purchasedListing);
			queryParams.put("fields", new String[] {"purchased_listing"});
			queryParams.put("from", 0);		// 从第1条结果记录开始
			queryParams.put("size", 11);	// 返回前10条结果记录（为了排除输入的用户自己，取11个结果）
			queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
			jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			iter = jnDocs.iterator();
			while (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				Long similarUserId = jnDoc.get("_source").get("user_id").asLong();
				if (similarUserId == userId) continue;
				
				users.append(similarUserId).append(" ");
				
			}
			System.out.println(
					String.format("该用户的最近邻是：%s", users.toString())
					);
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
		
		// 根据最近邻的用户ID，查找推荐商品
		try {
			Map<String, Object> queryParams = new HashMap<>();
			
			queryParams.put("index", "listing_vs_user");
			queryParams.put("type", "listing");
			
			queryParams.put("query", users.toString());
			queryParams.put("fields", new String[] {"purchased_users"});
			queryParams.put("from", 0);		// 从第1条结果记录开始
			queryParams.put("size", 10);	// 返回前10条结果记录
			queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
			
			// 获取排名靠前的商品 
			JsonNode jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			Iterator<JsonNode> iter = jnDocs.iterator();
			while (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				Long listingIdRecom = jnDoc.get("_source").get("listing_id").asLong();
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
		
		ElasticsearchUserBasedCF eucf = new ElasticsearchUserBasedCF(serverParams);
		
		while (true) {
			BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
            System.out.print("请输入用户ID：");  
            String content = strin.readLine();
            
            if ("exit".equalsIgnoreCase(content)) break;
			eucf.recommend(Long.parseLong(content));
		}
		
		eucf.cleanup();
		

	}

}
