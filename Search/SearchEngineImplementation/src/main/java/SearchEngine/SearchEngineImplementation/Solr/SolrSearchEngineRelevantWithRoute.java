package SearchEngine.SearchEngineImplementation.Solr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import MahoutMachineLearning.Classification.NBQueryClassifierOnlineForSearch;
import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class SolrSearchEngineRelevantWithRoute implements SearchEngineBasicInterface{
	
	protected SolrSearchEngineBasic sseb = null;
	protected NBQueryClassifierOnlineForSearch nbqcsearch = null;
	private HashMap<String, String> category2route = new HashMap<>();
	
	public SolrSearchEngineRelevantWithRoute(SolrSearchEngineBasic sseb) {
		
		// 基本的搜索引擎不变
		this.sseb = sseb;
		
		// 增加了查询分类模块
		nbqcsearch = new NBQueryClassifierOnlineForSearch();
		
		// 初始化分类到路由的映射
		// 消费电子
		category2route.put("手机", "ce");
		category2route.put("电脑", "ce");
		// 日用品
		category2route.put("美发护发", "daily");
		category2route.put("沐浴露", "daily");
		category2route.put("大米", "daily");
		category2route.put("食用油", "daily");
		category2route.put("面粉", "daily");
		
		// 饮料和零食
		category2route.put("坚果", "drinksnack");
		category2route.put("巧克力", "drinksnack");
		category2route.put("饼干", "drinksnack");
		category2route.put("饮料饮品", "drinksnack");
		category2route.put("方便面", "drinksnack");
		
		// 生鲜和干货
		category2route.put("海鲜水产", "freshdry");
		category2route.put("新鲜水果", "freshdry");
		category2route.put("纯牛奶", "freshdry");
		category2route.put("进口牛奶", "freshdry");
		category2route.put("枣类", "freshdry");
		category2route.put("茶叶", "freshdry");
		
	}
	
	public void cleanup() {
		sseb.cleanup();
		nbqcsearch.cleanup();	// 增加了查询分类的资源回收
	}
	
	// 索引部分加入了route路由机制
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		UpdateResponse response = null;
		
		try {
		
			// 适配部分：根据输入的统一文档ListingDocument，生成并添加Solr所使用的SolrInputDocument
			for (ListingDocument ld : documents) {
	
				SolrInputDocument sid = new SolrInputDocument();
				
				// 索引时为路由加入定制的前缀，这里使用category2route中的定义，包括ce、daily、drinksnack和freshdry
				sid.addField("listing_id", String.format("%s!%s", category2route.get(ld.getCategory_name()), ld.getListing_id()));
				
				// 其他字段不变
				sid.addField("listing_title", ld.getListing_title());
				sid.addField("category_id", ld.getCategory_id());
				sid.addField("category_name", ld.getCategory_name());
				
				sseb.solrClient.add(sid);
			}
			
			// 写入Solr Cloud的索引
			response = sseb.solrClient.commit();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return response.toString();
		
	}
	
	

	// 查询部分附加上相关性、以及基于路由的逻辑
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
			sq.set("q", "*:*");
			sq.set("fq", sbQuery.toString());	// 使用过滤查询
			sq.set("qf", sbQf.toString());
			sq.set("start", start);
			sq.set("rows", rows);
			
			// 原有的装饰部分：通过查询分类的结果，优化相关性
			// 如下这行可以使用RESTful API或者服务化模块代替，这样模块间耦合度更低
			HashMap<String, Double> queryClassificationResults 
				= (HashMap<String, Double>) nbqcsearch.predict(query);
			
			// 由于路由数量少，查找快，所以routes没有使用哈希表
			ArrayList<String> routes = new ArrayList<>();
			StringBuffer sbRoutes = new StringBuffer();
			
			for (String cate : queryClassificationResults.keySet()) {
				double score = queryClassificationResults.get(cate);
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				sq.add("bq", String.format("category_name:(%s^%f)", cate, score));
				
				// 新增的装饰部分：根据分类获取所有路由route
				String route = category2route.get(cate);
				if (!routes.contains(route)) {
					routes.add(route);
					sbRoutes.append(route).append("!,");
				}
//				System.out.println(String.format("category_name:(%s^%f)", cate, score));
			}
			
			// 新增的装饰部分：根据分类指定route
			sq.add("_route_", sbRoutes.toString()); 
			
			// 获取查询结果
			response = sseb.solrClient.query(sq);
//			SolrDocumentList list = response.getResults();
//			。。。这里略去后续统一文档拼装的实现。。。
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
		
		return response.toString();
	}
	
	public void indexListing(String file, Map<String, Object> indexParams) {
		
		ArrayList<ListingDocument> documents = new ArrayList<>();
		
		try {
			
			// 读取原始Listing商品数据文件，拼装ListingDocument
			BufferedReader br = new BufferedReader(new FileReader(file));
			String strLine = null;
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				
				ListingDocument ld = new ListingDocument(Long.parseLong(tokens[0]), tokens[1], 
						Long.parseLong(tokens[2]), tokens[3]);
				documents.add(ld);
			}
			
			br.close();
			
			System.out.println("start to index...");
			this.index(documents, indexParams);
			System.out.println("finished");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// 连接Solr Cloud的Zookeeper设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
		serverParams.put("zkHost",  "192.168.1.48:9983");
		// 查询读取哪个Collection，根据你的需要设置。
		serverParams.put("collection",  "listing_collection_withroute");
		
		// 创建基本款搜索引擎
		SolrSearchEngineBasic sseb = new SolrSearchEngineBasic(serverParams);
		// 使用装饰器模式设计的新搜索引擎
		SolrSearchEngineRelevantWithRoute sserr 
			= new SolrSearchEngineRelevantWithRoute(sseb);
		
		// 测试基于路由的索引
//		sserr.indexListing("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm"
//				+ "/listing-segmented-shuffled-noheader.txt", serverParams);
		
		
		Map<String, Object> queryParams = new HashMap<>();
		// 查询关键词
		queryParams.put("query", "米");
		queryParams.put("fields", 
				new String[] {"listing_title"});
		queryParams.put("start", 0);	// 从第1条结果记录开始
		queryParams.put("rows", 5);		// 返回5条结果记录
		
		
		// 对比相关性改善前后
		System.out.println("基础搜索：\t\t\t" + sseb.query(queryParams));
		System.out.println("相关性改良、基于路由的搜索：\t" + sserr.query(queryParams));
		
		sseb.cleanup();
		sserr.cleanup();
		
	}

}
