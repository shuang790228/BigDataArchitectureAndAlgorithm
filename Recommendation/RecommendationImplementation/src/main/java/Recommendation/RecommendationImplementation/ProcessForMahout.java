package Recommendation.RecommendationImplementation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ProcessForMahout {
	
	// 预处理用户购买记录，将其转化为Mahout能够处理的文件格式
	public static void process(String inputFileName, String outputFileName) {
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(inputFileName));
			PrintWriter pw = new PrintWriter(new FileWriter(outputFileName));
			String strLine = br.readLine();		// 跳过header行
			while ((strLine = br.readLine()) != null) {
				
				// 统计每个商品购买的次数，假设购买次数越多，用户对其喜好度越高
				Map<Long, Integer> counters = new HashMap<>();
				String[] tokens = strLine.split("\t");
				String userId = tokens[0];
				String[] listingIds = tokens[1].split(" ");
				for (String listingId : listingIds) {
					Long llistingId = Long.parseLong(listingId);
					if (!counters.containsKey(llistingId)) {
						counters.put(llistingId, 1);
					} else {
						counters.put(llistingId, counters.get(llistingId) + 1);
					}
				}
				
				// 按照Mahout要求的格式写入文件
				for (Long llistingId : counters.keySet()) {
					pw.print(String.format("%s,%s,%d\r\n", userId, llistingId, counters.get(llistingId)));
				}
				
				
			}
			
			
			pw.close();
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		process("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/user-purchases.txt", 
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/user-purchases.mahout.csv");
		
	}

}
