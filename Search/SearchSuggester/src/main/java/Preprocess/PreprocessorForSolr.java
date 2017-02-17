package Preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import MahoutMachineLearning.Classification.NBQueryClassifierOnlineForSearch;

public class PreprocessorForSolr {
	
	Analyzer ikanalyzer = null;
    TokenStream ts = null;
    NBQueryClassifierOnlineForSearch nbqcsearch = null;
	
    public PreprocessorForSolr() {
    	ikanalyzer = new IKAnalyzer();
    	nbqcsearch = new NBQueryClassifierOnlineForSearch();
    }
    
    public void cleanup() {
    	ikanalyzer.close();
    	nbqcsearch.cleanup();
    }
    
    // 获得汉字的拼音
 	private String getPinyin(String hanzi) {
 		return pinyin4j.makeStringByStringSet(pinyin4j.getPinyin(hanzi));
 	}
 	
 	// 获得汉字的首拼
 	private String getShoupin(String hanzi) {
 		
 		StringBuffer sbShoupin = new StringBuffer();
 		
 		char[] chars = hanzi.toCharArray();
 		
 		// 由于存在多音字的可能，处理所有可能的发音
 		String[] allpossiblepinyins = new String[chars.length];
 		
 		for (int i = 0; i < chars.length; i++) {
 			char c = chars[i];
 			if (c == ' ') continue;
 			String[] pinyins = 
 					pinyin4j.makeStringByStringSet(pinyin4j.getPinyin(c + "")).split(",");
 			for (String pinyin : pinyins) {
 				if ("".equals(pinyin)) continue;
 				if (allpossiblepinyins[i] == null) 
 					allpossiblepinyins[i] = "" + pinyin.charAt(0);
 				else {
 					if (allpossiblepinyins[i].indexOf(pinyin.charAt(0)) < 0) 
 						allpossiblepinyins[i] += pinyin.charAt(0);
 				}
 			}
 			
 		}
 		
 	// 这里暂时不考虑多音字的合理性，通过递归的方式，获取所有可能的首拼组合
 		getAllShouPin(sbShoupin, allpossiblepinyins, "", allpossiblepinyins.length);
 		
 		return sbShoupin.toString().trim();
 		
 	}
 	
 	// 这里暂时不考虑多音字的合理性，通过递归的方式，获取所有可能的首拼组合
 	private boolean getAllShouPin(StringBuffer sbShoupin, String[] allpossiblepinyins, String current, int rest) {
		
		if (rest == 0) {
			sbShoupin.append(current).append(" ");
			return false;
		}
		
		if (allpossiblepinyins[allpossiblepinyins.length - rest] == null) return false;
		
		char[] chars = allpossiblepinyins[allpossiblepinyins.length - rest].toCharArray();
		rest --;
		for (char shoupinchar : chars) {
			getAllShouPin(sbShoupin, allpossiblepinyins, current + shoupinchar, rest);
		}
		
		return true;
		
	}
 	
    
    
	// 将查询日志querylog.examples.txt转化成solr的输入文件格式
	public void prepareSolrInput(String querylogFile, String forSolrFile) {
		
		BufferedWriter bwForSolrIndexing = null;
		
		try {
		
			bwForSolrIndexing = new BufferedWriter
					(new OutputStreamWriter(new FileOutputStream(forSolrFile), "utf-8"));
			bwForSolrIndexing.write("<add>\r\n");
			
			// 读取查询日志
			int id = 0;
			BufferedReader br = new BufferedReader
					(new InputStreamReader(new FileInputStream(querylogFile), "utf-8"));
			String strLine = br.readLine();		// 跳过头部
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				if (tokens.length < 3) continue;
				
				// 读取每行的关键词、查询频率以及对应的商品数量
				String keyword = tokens[0];
				long frequency = 0, skunum = 0;
				frequency = Long.parseLong(tokens[1]);
				skunum = Long.parseLong(tokens[2]);
				
				Map<String, Integer> dedup = new HashMap<>();	// 用于去重
				List<String> keywordtokens = new ArrayList<String>(); 
				keywordtokens.add(keyword);	//首先加入原有关键词
				dedup.put(keyword, 1);
				
				// 除了原有关键词本身，加入关键词的中文分词
				ts = ikanalyzer.tokenStream("myfield", new StringReader(keyword));
			    //获取词元文本属性
			    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
			    //重置TokenStream（重置StringReader）
				ts.reset(); 
				//迭代获取分词结果
				while (ts.incrementToken()) {
					String termStr = term.toString();
					if (!dedup.containsKey(termStr)) {
						keywordtokens.add(term.toString());
						dedup.put(termStr, 1);
					}
				}
				//关闭TokenStream（关闭StringReader）
				ts.end(); 
				ts.close();
				
				// 获取分词、拼音以及首拼的前缀
				StringBuffer sbKeywordTokens = new StringBuffer();
				StringBuffer sbPinyinTokens = new StringBuffer();
				StringBuffer sbShouPinTokens = new StringBuffer();
				
				dedup.clear();
				for (String keywordtoken : keywordtokens) {
					
					// 处理中文分词
					for (int i = 1; i < keywordtoken.length() + 1; i++) {
						String prefix = keywordtoken.substring(0, i);
						if (!dedup.containsKey(prefix)) {
							sbKeywordTokens.append(prefix).append(" ");
							dedup.put(prefix, 1);
						}
					}
					
					// 处理拼音，存在多音字的可能
					String[] pinyintokens = getPinyin(keywordtoken).split(",");
					for (String pinyintoken : pinyintokens) {
						for (int i = 1; i < pinyintoken.length() + 1; i++) {
							String prefix = pinyintoken.substring(0, i);
							if (!dedup.containsKey(prefix)) {
								sbPinyinTokens.append(prefix).append(" ");
								dedup.put(prefix, 1);
							}
						}
						
					}
					
					// 处理首拼
					String shoupintoken = getShoupin(keywordtoken);
					for (int i = 1; i < shoupintoken.length() + 1; i++) {
						String prefix = shoupintoken.substring(0, i);
						if (!dedup.containsKey(prefix)) {
							sbPinyinTokens.append(prefix).append(" ");
							dedup.put(prefix, 1);
						}
					}
					
				}
				
				
				// 对查询进行分类，获取最相关的2个分类
				Map<String, Double> prediction = nbqcsearch.predict(keyword);
				List<Pair> rank = new ArrayList<>();
				for (String category : prediction.keySet()) {
					rank.add(new Pair(category, prediction.get(category), true));
				}
				Collections.sort(rank);
				System.out.println(rank.get(0).strToken);
				
				
				// 构建用于Solr索引的xml文件
				bwForSolrIndexing.write("\t<doc>\r\n");
				bwForSolrIndexing.write(String.format("\t\t<field name = \"id\">%d</field>\r\n", 
						id));
				bwForSolrIndexing.write(String.format("\t\t<field name = \"query\">%s</field>\r\n", 
						keyword.trim()));
				bwForSolrIndexing.write(String.format("\t\t<field name = \"term_prefixs\">%s</field>\r\n", 
						sbKeywordTokens.toString()));
				bwForSolrIndexing.write(String.format("\t\t<field name = \"pinyin_prefixs\">%s %s</field>\r\n", 
						sbPinyinTokens.toString(), sbShouPinTokens.toString()));
				
				// 设置阈值0.2，过滤不相干的分类
				// 注意，由于我们的分类训练样本有限，所以某些查询无法获得相应的分类预测结果，或者是预测结果不准，
				// 这点可以通过加大训练样本来改善。
				if (rank.get(0).dWeight > 0.2) {
					bwForSolrIndexing.write(String.format("\t\t<field name = \"category\">%s</field>\r\n", 
						rank.get(0).strToken));
					if (rank.get(1).dWeight > 0.2) {
						bwForSolrIndexing.write(String.format("\t\t<field name = \"category\">%s</field>\r\n", 
							rank.get(1).strToken));
					}
				}
				
				bwForSolrIndexing.write(String.format("\t\t<field name = \"frequency\">%s</field>\r\n", 
						frequency));
				bwForSolrIndexing.write(String.format("\t\t<field name = \"skunum\">%d</field>\r\n", 
						skunum));
				bwForSolrIndexing.write("\t</doc>\r\n");
				bwForSolrIndexing.flush();
				
				
				id++;
				
			}
			
			
			br.close();
			
			
			bwForSolrIndexing.write("</add>\r\n");
			bwForSolrIndexing.close();
			
			
			
		} catch (Exception ex) {
			
			if (bwForSolrIndexing != null) {
				try {
					bwForSolrIndexing.flush();
					bwForSolrIndexing.close();
				} catch (Exception ex2) {
					ex2.printStackTrace();
				}
			}
			ex.printStackTrace();
		}
		
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		PreprocessorForSolr preprocessor = new PreprocessorForSolr();
		preprocessor.prepareSolrInput("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/querylog.examples.txt", 
				"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/querylog.forsolr.xml");
		
		preprocessor.cleanup();
	}

}
