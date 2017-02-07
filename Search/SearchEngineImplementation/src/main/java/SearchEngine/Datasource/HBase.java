package SearchEngine.Datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineTest;

public class HBase {
	
	// 基本配置和连接 
	private static Configuration conf = null;
	private static Connection conn = null;
	private static HTable htable = null;
	
	// HBase连接的相关配置和初始化
	public static synchronized void init() {
		
		if (conf == null || conn == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set("hbase.zookeeper.quorum", "192.168.1.48:2181");
			conf.set("hbase.master", "16000");
			
			try {
				conn = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	// 释放资源
	public static void cleanup() {
		if (conn != null) {
			try {
				conn.close();
				conn = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn = null;
			}
		}
		
		if (conf != null) {
			conf.clear();
			conf = null;
		}
	}
	
	public static void insertData(List<ListingDocument> lds, String table) {
		
		try {
		
			// 连接指定的HBase表格
			htable = (HTable) conn.getTable(TableName.valueOf(table));
			
			List<Put> puts = new ArrayList<>();
			for (ListingDocument ld : lds) {	//使用HBase的PUT API，每个文档生成一个PUT
				
				// 添加必要的商品基础信息
				Put put = new Put(String.valueOf(ld.getListing_id()).getBytes());
				put.addColumn("datafields".getBytes(), 
						"listing_title".getBytes(), 
						ld.getListing_title().getBytes());
				put.addColumn("datafields".getBytes(), 
						"category_id".getBytes(), 
						String.valueOf(ld.getCategory_id()).getBytes());
				put.addColumn("datafields".getBytes(), 
						"category_name".getBytes(), 
						ld.getCategory_name().getBytes());
				
				// 如果存在，添加促销的信息
				if (ld.getPromotion_info() != null) {
					put.addColumn("datafields".getBytes(), 
							"promotion_info".getBytes(), 
							ld.getPromotion_info().getBytes());
					put.addColumn("datafields".getBytes(), 
							"promotion_startdate".getBytes(), 
							ld.getPromotion_startdate().getBytes());
					put.addColumn("datafields".getBytes(), 
							"promotion_enddate".getBytes(), 
							ld.getPromotion_enddate().getBytes());
				}
				
				// 如果存在，添加团购的信息
				if (ld.getGroup_discount() != -1.0) {
					put.addColumn("datafields".getBytes(), 
							"group_discount".getBytes(), 
							String.valueOf(ld.getGroup_discount()).getBytes());
					put.addColumn("datafields".getBytes(), 
							"group_startdate".getBytes(), 
							ld.getGroup_startdate().getBytes());
					put.addColumn("datafields".getBytes(), 
							"group_enddate".getBytes(), 
							ld.getGroup_enddate().getBytes());
				}
				
				puts.add(put);
			}
			
			htable.put(puts);
			
			htable.close();
			htable = null;
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			
			if (htable != null) {
				try {
					htable.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					htable = null;
				}
				htable = null;
			}
		}
		
	}
	
	// 给定时间戳timestamp，找出这个时间戳之后修改的所有数据
	public static List<ListingDocument> scanUpdatedData(String table, long timestamp) {
		
		List<ListingDocument> lds = new ArrayList<>();
		
		try {
			htable = (HTable) conn.getTable(TableName.valueOf(table));
			ResultScanner rscan = null;
			
			// 设置时间戳
			Scan scanWithFilter = new Scan();
			scanWithFilter.setTimeRange(timestamp, System.currentTimeMillis());
			
			// 使用HBase的Scan机制
			rscan = htable.getScanner(scanWithFilter);
			for (Result res : rscan) {
				
				ListingDocument ld = new ListingDocument();
				ld.setListing_id(Long.parseLong(new String(res.getRow())));
				
				// 读取各个字段，组装ListingDocument。之前HBase简介中讲述了cell这种结构。
				for (Cell cell : res.rawCells()) {
					String columnFamily = new String(CellUtil.cloneFamily(cell));
					if ("datafields".equalsIgnoreCase(columnFamily)) {
						
						String qualifier = new String(CellUtil.cloneQualifier(cell));
						
						if ("listing_title".equalsIgnoreCase(qualifier)) {
							ld.setListing_title(new String(CellUtil.cloneValue(cell)));
						} else if ("category_id".equalsIgnoreCase(qualifier)) {
							ld.setCategory_id(Long.parseLong(new String(CellUtil.cloneValue(cell))));
						} else if ("category_name".equalsIgnoreCase(qualifier)) {
							ld.setCategory_name(new String(CellUtil.cloneValue(cell)));
						} else if ("promotion_info".equalsIgnoreCase(qualifier)) {
							ld.setPromotion_info(new String(CellUtil.cloneValue(cell)));
						} else if ("promotion_startdate".equalsIgnoreCase(qualifier)) {
							ld.setPromotion_startdate(new String(CellUtil.cloneValue(cell)));
						} else if ("promotion_enddate".equalsIgnoreCase(qualifier)) {
							ld.setPromotion_enddate(new String(CellUtil.cloneValue(cell)));
						} else if ("group_discount".equalsIgnoreCase(qualifier)) {
							ld.setGroup_discount(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
						} else if ("group_startdate".equalsIgnoreCase(qualifier)) {
							ld.setGroup_startdate(new String(CellUtil.cloneValue(cell)));
						} else if ("group_enddate".equalsIgnoreCase(qualifier)) {
							ld.setGroup_enddate(new String(CellUtil.cloneValue(cell)));
						}
						
					}
				}
				
				lds.add(ld);
				System.out.println(ld.toString());	// 用于检阅的输出
				
			}
			
			
			htable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		
		return lds;
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		HBase.init();

		// 向HBase中写入测试数据
		Random rand = new Random(System.currentTimeMillis());
		List<ListingDocument> documents = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			ListingDocument ld = new ListingDocument(
					2000 + (i + 1), "hbase数据插入测试标题" + (i + 1), 
					200000 + (i + 1), "hbase数据插入测试类目" + (i + 1));
			
			int number = rand.nextInt(10);
			
			// 按照20%的概率，随机生成促销商品
			if (number % 5 == 0) {
			
				// 按照10%的概率，随机生成促销类型A的商品
				if (number % 2 == 0) {
					ld.setPromotion_info("满200减50");
					ld.setPromotion_startdate("2017-07-28");
					ld.setPromotion_enddate("2017-08-28");
				} else {	// 按照10%的概率，随机生成促销类型B的商品
					ld.setPromotion_info("满三赠一");
					ld.setPromotion_startdate("2017-08-01");
					ld.setPromotion_enddate("2017-08-18");
				}
				
			} else if (number % 10 == 1) { // 按照10%的概率，随机生成团购的商品
				ld.setGroup_discount(0.75);
				ld.setGroup_startdate("2017-10-05");
				ld.setGroup_enddate("2017-10-12");
			}
			
			documents.add(ld);
		}
		
		HBase.insertData(documents, "listing_segmented_shuffled_inhbase");
		
		
		
		// 查找刚刚插入的数据
		long timestamp = System.currentTimeMillis() - 3600 * 1000;	// 查找1小时内更新的数据
		List<ListingDocument> documentsToUpdate = HBase.scanUpdatedData("listing_segmented_shuffled_inhbase", timestamp);
		
		
		// 写入搜索引擎
		SearchEngineTest.init();
		SearchEngineTest.index(documentsToUpdate);	//索引新文档
		SearchEngineTest.cleanup();
		
		HBase.cleanup();
	}

}
