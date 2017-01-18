/**
 * IK 中文分词  版本 5.0.1
 * IK Analyzer release 5.0.1
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * 源代码由林良益(linliangyi2005@gmail.com)提供
 * 版权声明 2012，乌龙茶工作室
 * provided by Linliangyi and copyright 2012 by Oolong studio
 * 
 * 
 */
package org.wltea.analyzer.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 使用IKAnalyzer对listing数据进行分词
 * 2016-12-25
 *
 */
public class IKAnalzyerForListing {
	
	 //构建IK分词器，使用smart分词模式
	private static Analyzer analyzer = new IKAnalyzer(true);
	
	//获取Lucene的TokenStream对象
	private static TokenStream ts = null;
	
	// 读取listing文件
	private static BufferedReader br = null;
	
	// 写入分词后的结果文件
	private static PrintWriter pw = null;
	
	public static void processListing(String inputFileName, String outputFileName) {
		
		try {
			
			br = new BufferedReader(new FileReader(inputFileName));
			pw = new PrintWriter(new FileWriter(outputFileName));
			
			String strLine = br.readLine();		//跳过header这行
			pw.println(strLine);
			while ((strLine = br.readLine()) != null) {
				
				//获取每个字段
				String[] tokens = strLine.split("\t");
				String id = tokens[0];
				String title = tokens[1];
				String cateId = tokens[2];
				String cateName = tokens[3];
			
				//对原有商品标题进行中文分词
				ts = analyzer.tokenStream("myfield", new StringReader(title));
				//获取词元位置属性
//			    OffsetAttribute  offset = ts.addAttribute(OffsetAttribute.class); 
			    //获取词元文本属性
			    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
			    //获取词元文本属性
//			    TypeAttribute type = ts.addAttribute(TypeAttribute.class);
			    
			    
			    //重置TokenStream（重置StringReader）
				ts.reset(); 
				//迭代获取分词结果
				StringBuffer sbSegmentedTitle = new StringBuffer();
				while (ts.incrementToken()) {
				  sbSegmentedTitle.append(term.toString()).append(" ");
				}
				
				//重新写入分词后的商品标题
				pw.println(String.format("%s\t%s\t%s\t%s", id, sbSegmentedTitle.toString().trim(), cateId, cateName));
				
				//关闭TokenStream（关闭StringReader）
				ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
				
			}

			br.close();
			br = null;
			
			pw.close();
			pw = null;
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cleanup();
	    }
		
		
	}
	
	public static void processListingWithShuffle(String inputFileName, String outputFileName) {
		
		try {
			
			br = new BufferedReader(new FileReader(inputFileName));
			pw = new PrintWriter(new FileWriter(outputFileName));
			
			ArrayList<String> samples = new ArrayList<String>();
			
			String strLine = br.readLine();		//跳过header这行
			pw.println(strLine);
			while ((strLine = br.readLine()) != null) {
				
				//获取每个字段
				String[] tokens = strLine.split("\t");
				String id = tokens[0];
				String title = tokens[1];
				String cateId = tokens[2];
				String cateName = tokens[3];
			
				//对原有商品标题进行中文分词
				ts = analyzer.tokenStream("myfield", new StringReader(title));
				//获取词元位置属性
//			    OffsetAttribute  offset = ts.addAttribute(OffsetAttribute.class); 
			    //获取词元文本属性
			    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
			    //获取词元文本属性
//			    TypeAttribute type = ts.addAttribute(TypeAttribute.class);
			    
			    
			    //重置TokenStream（重置StringReader）
				ts.reset(); 
				//迭代获取分词结果
				StringBuffer sbSegmentedTitle = new StringBuffer();
				while (ts.incrementToken()) {
				  sbSegmentedTitle.append(term.toString()).append(" ");
				 
				
				}
				
				
				samples.add(String.format("%s\t%s\t%s\t%s", id, sbSegmentedTitle.toString().trim(), cateId, cateName));
				
				//关闭TokenStream（关闭StringReader）
				ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
				
			}
			br.close();
			br = null;
					
			
			Random rand = new Random(System.currentTimeMillis());
			while (samples.size() > 0) {
				int index = rand.nextInt(samples.size());
				pw.println(samples.get(index));
				samples.remove(index);
			}
			
			
				
			
			pw.close();
			pw = null;
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cleanup();
	    }
		
		
	}
	
	public static void cleanup() {
		
		if (br != null) {
			try {
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if (pw != null) {
			try {
				pw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//释放TokenStream的所有资源
		if(ts != null){
	      try {
			ts.close();
			ts = null;
	      } catch (IOException e) {
			e.printStackTrace();
	      }
		}
		
		if (analyzer != null) {
			try {
				analyzer.close();
				analyzer = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args){
		
		processListing("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing.txt",
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented.txt");
		
		processListingWithShuffle("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing.txt",
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled.txt");
	    
	}

}
