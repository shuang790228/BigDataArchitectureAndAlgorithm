package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ProcessForUserPurchase {
	
	
	public void process(String purchaseFileName, String outputFileName) {
		
		PrintWriter pw = null;
		 String jsonLine1 = "{ \"index\" : { \"_index\" : \"user_vs_listing\", \"_type\" : \"user\" } }";
		
		// 加载用户购买的记录
		try {
			
			 // 保存输出的文件
            pw = new PrintWriter(new FileWriter(outputFileName));
			
			BufferedReader br = new BufferedReader(new FileReader(purchaseFileName));
			String strLine = br.readLine();  // 跳过header行
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				if (tokens.length < 2) continue;
				
				Long userId = Long.parseLong(tokens[0]);
				String listingIds = tokens[1];
				
				String jsonLine2 = String.format("{ \"user_id\" : \"%d\", \"purchased_listing\" : \"%s\"}",
						userId, 
						listingIds);
            	
            	// 将JSON对象写入输出文件
            	pw.println(jsonLine1);
            	pw.println(jsonLine2);
				
			}
			
			br.close();
			pw.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {		//最后的扫尾工作
			if (pw != null) pw.close();
		}
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ProcessForUserPurchase pfup = new ProcessForUserPurchase();
		pfup.process(
        		"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/user-purchases.txt",
        		"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/userpurchases-for-elasticsearch.txt");
		

	}

}
