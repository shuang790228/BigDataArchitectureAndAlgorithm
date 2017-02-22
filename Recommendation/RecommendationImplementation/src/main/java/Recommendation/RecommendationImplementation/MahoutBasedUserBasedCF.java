package Recommendation.RecommendationImplementation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class MahoutBasedUserBasedCF {
	
	DataModel model = null;
	UserSimilarity is = null;
	UserNeighborhood un = null;
	GenericUserBasedRecommender gur = null;
	Map<Long, String> listingId2LisintTitle = new HashMap<>();
	
	//初始化
	public MahoutBasedUserBasedCF(String fileName) {
		
		try {
			model = new FileDataModel(new File(fileName));
			is = new EuclideanDistanceSimilarity(model);	// 基于皮尔逊相关系数的相似度
			un = new NearestNUserNeighborhood(5, is, model);		// 选取至多5个最近邻
			gur = new GenericUserBasedRecommender(model, un, is);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// 出于显示的目的，加载商品列表到内存。对实际生产中的大规模数据而言，可能需要查询数据库、搜索引擎等持久化存储
	public void loadListing(String fileName) {
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String strLine = null;
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				Long listingId = Long.parseLong(tokens[0]);
				String listingTitle = tokens[1];
				
				listingId2LisintTitle.put(listingId, listingTitle);
			}
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	
	
	// 通过给定的用户ID和基于用户的协同过滤，进行推荐
	public List<Long> recommend(Long userID) {
		
		List<Long> listingIds = new ArrayList<>();
		
		try {
			
			List<RecommendedItem> recommItems = gur.recommend(userID, 10);
			
			System.out.println(String.format("基于用户的协同过滤-推荐商品是："));
			for (RecommendedItem ri : recommItems) {
				listingIds.add(ri.getItemID());
				System.out.println(String.format("\t%s", listingId2LisintTitle.get(ri.getItemID())));
			}
			
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return listingIds;
		
	}
	
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		MahoutBasedUserBasedCF mucf = new MahoutBasedUserBasedCF(
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/user-purchases.mahout.csv");
		
		// loadListing函数仅供显示之用
		mucf.loadListing("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-noheader.txt");
		
		while (true) {
			BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
            System.out.print("请输入用户ID：");  
            String content = strin.readLine();
            
            if ("exit".equalsIgnoreCase(content)) break;
            
            long start = System.currentTimeMillis();
			mucf.recommend(Long.parseLong(content));
			long end = System.currentTimeMillis();
			System.out.println(String.format("耗时%f秒", (end - start) / 1000.0));
		}
		
	}

}
