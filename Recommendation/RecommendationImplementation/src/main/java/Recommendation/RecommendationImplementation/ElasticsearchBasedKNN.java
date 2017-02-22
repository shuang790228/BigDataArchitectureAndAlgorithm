package Recommendation.RecommendationImplementation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import SearchEngine.SearchEngineImplementation.Elasticsearch.ElasticSearchEngineBasic;

public class ElasticsearchBasedKNN {
	
	ElasticSearchEngineBasic ese = null;
	ObjectMapper mapper = null;
	
	public ElasticsearchBasedKNN(Map<String, Object> serverParams) {
		ese = new ElasticSearchEngineBasic(serverParams);
		mapper = new ObjectMapper();
	}
	
	// 根据输入的商品标题，预测其分类
	public String predict(String title, Long id) {
		
		Map<String, Object> queryParams = new HashMap<>();
		
		// 和Solr有所不同，需要在这里指定索引和类型
		queryParams.put("index", "listing_new");
		queryParams.put("type", "listing");
		
		// 查询关键词
		queryParams.put("query", title);
		// 仅仅在标题字段上查询
		queryParams.put("fields", new String[] {"listing_title"});
		queryParams.put("from", 0);		// 从第1条结果记录开始
		queryParams.put("size", 11);	// 返回前10条结果记录（为了排除输入的商品自己，取11个结果）
		queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式

		// 统计每个分类的商品数量
		Map<String, Integer> counters = new HashMap<>();
		try {
			JsonNode jnDocs = mapper.readValue(ese.query(queryParams), JsonNode.class)
					.get("hits").get("hits");
			Iterator<JsonNode> iter = jnDocs.iterator();
			while (iter.hasNext()) {
				JsonNode jnDoc = iter.next();
				// 由于搜索引擎中包含输入商品本身，排除它自己。也可以修改ElasticSearchEngineBasic的实现，使用filter来排除
				if (jnDoc.get("_source").get("listing_id").asLong() == id) continue;
				
				String categoryName = jnDoc.get("_source").get("category_name").asText();
				if (!counters.containsKey(categoryName)) {
					counters.put(categoryName, 1);
				} else {
					counters.put(categoryName, counters.get(categoryName) + 1);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
		
		// 获取商品数量最多的分类，将其作为输入商品的预测分类
		int max = Integer.MIN_VALUE;
		String label = "";
		for (String categoryName : counters.keySet()) {
			int count = counters.get(categoryName);
			if (count > max) {
				max = count;
				label = categoryName;
			}
		}
		
		
		return label;
		
	}
	
	// 遍历整个商品列表，获取准确率
	public double getAccuracy(String fileName) {
		
		try {
			
			int totalCount = 0, correctCount = 0;
			
			// 遍历商品列表
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String strLine = null;
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				Long listingId = Long.parseLong(tokens[0]);
				String listingTitle = tokens[1];
				String categoryName = tokens[3];
				
				// 如果预测的分类和商品真实分类一致，认为正确
				if (categoryName.equalsIgnoreCase(predict(listingTitle, listingId))) {
					correctCount ++;
				}
				
				totalCount ++;
				if (totalCount % 1000 == 0) {
					System.out.println(String.format("已完成 %d 次预测", totalCount));
				}
			}
			br.close();
			
			
			// 返回准确率
			return ((double) correctCount) / totalCount;
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			
			return -1.0;
		}
		
	}
	
	public void cleanup() {
		ese.cleanup();
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Elasticsearch服务器的设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
		serverParams.put("server",  new byte[]{(byte)192,(byte)168,1,48});
		serverParams.put("port", 9300);
		serverParams.put("cluster", "ECommerce");
		
		ElasticsearchBasedKNN ebknn = new ElasticsearchBasedKNN(serverParams);
		
		System.out.println(
				ebknn.getAccuracy("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-noheader.txt")
				);
		
		ebknn.cleanup();
		

	}

}
