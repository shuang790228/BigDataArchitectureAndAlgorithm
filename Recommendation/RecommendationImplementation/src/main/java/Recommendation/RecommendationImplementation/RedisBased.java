package Recommendation.RecommendationImplementation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisBased {
	
	private JedisPool jedisPool = null;	//非切片连接池
	private Jedis jedis = null;	// 连接客户端
	// 商品ID到商品标题和分类信息的映射
	private Map<Long, String> listingId2tilteAndCate = new HashMap<>(); 
	// 样本ID到商品ID的映射
	private Map<Long, Long> sampleId2listingId = new HashMap<>();
	private Random rand = new Random();
	
    private RedisBased() 
    { 
        // 连接池的基本配置 
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxTotal(20); 
        config.setMaxIdle(5); 
        config.setMaxWaitMillis(1000l); 
        config.setTestOnBorrow(false); 

        // 根据你的需要设置IP/主机和端口
        jedisPool = new JedisPool(config,"127.0.0.1",6379);
        // 获取连接资源
        jedis = jedisPool.getResource();
        
    } 
   
    
    public void cleanup() {
    	if (jedis != null) {
    		jedis.close();
    	}
    }
	
	public void load(String listingFile, String clusterFile) {
		
		try {
			
			// 加载商品数据
			BufferedReader br = new BufferedReader(new FileReader(listingFile));
			String strLine = br.readLine();	// 跳过header这行
			long sampleId = 1L;
			while ((strLine = br.readLine()) != null) {
				
				String[] tokens = strLine.split("\t");
				if (tokens.length < 4) continue;
				Long listingId = Long.parseLong(tokens[0]);
				String title = tokens[1];
				String cate = tokens[3];
				// 加载商品ID到商品标题和分类的信息
				listingId2tilteAndCate.put(listingId, String.format("标题：%s\t\t分类：%s", title, cate));
				
				// 从样本ID到商品ID的映射
				sampleId2listingId.put(sampleId, listingId);
				sampleId ++;
			
			}
			
			br.close();
			
			// 加载聚类数据
			jedis.flushDB();		// 清空原有数据，慎用
			br = new BufferedReader(new FileReader(clusterFile));
			strLine = br.readLine();	// 跳过header这行
			sampleId = 1;
			while ((strLine = br.readLine()) != null) {
				
				String[] tokens = strLine.split("\t");
				String clusterId = String.format("cluster_%s", tokens[1]);
				Long listingId = sampleId2listingId.get(sampleId);
				// 第一个映射：从商品ID到聚类ID，以列表形式存储是出于扩展性的考虑，一件商品可能属于多个聚类
				jedis.rpush(listingId.toString(), clusterId);
				
				// 第二个映射：从聚类ID到所有属于该聚类的商品列表
				jedis.rpush(clusterId, listingId.toString());
				
				sampleId ++;
				
			}
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	public List<String> recommend(Long listingId) {
		
		// 获取该商品属于的聚类
		String clusterId = jedis.lrange(listingId.toString(), 0, 1).get(0);
		
		// 从聚类的商品列表中，随机挑选10件商品
		long len = jedis.llen(clusterId);
		long start = rand.nextInt((int)(len - 10));
		return jedis.lrange(clusterId, start, start + 10);
		
	}
	
	public String getListingInfo(Long similarListingId) {
		return listingId2tilteAndCate.get(similarListingId);
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		RedisBased rb = new RedisBased();
		
		// 加载聚类数据到Redis
		rb.load("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled.txt", 
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing.clusters.txt");
		
		// 随机挑选3个商品，获取推荐
		Random rand = new Random();
		int n = 3;
		for (int i = 0; i < n; i++) {
			Long listingId = (long) rand.nextInt(28706);
			System.out.println(String.format("输入的商品：%s\t%s", listingId, rb.getListingInfo(listingId)));
			List<String> similarListingIds = rb.recommend(listingId);
			for (String similarListingId : similarListingIds) {
				System.out.println(String.format("\t%s\t%s", 
						similarListingId, rb.getListingInfo(Long.parseLong(similarListingId))));
			}
			
			System.out.println();
		}
		
		rb.cleanup();
		

	}

}
