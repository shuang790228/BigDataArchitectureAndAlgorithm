package MahoutMachineLearning.Classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector.Element;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class NBQueryClassifierOnlineForSearch {
	
	private StandardNaiveBayesClassifier  classifier = null;
	private Map<Integer, String> labels = null;
	private Map<String, Integer> dictionary = null;
	private Map<String, String> categoryMapping = null;
	HashMap<String, HashMap<String, Double>> term2category = null;
	Analyzer ikanalyzer = null;
    TokenStream ts = null;
	
	
	public NBQueryClassifierOnlineForSearch() {
		
		try {
		
			//指定Mahout朴素贝叶斯分类模型的目录、类标文件和字典文件
	        String modelPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-model/";
	        String labelIndexPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-mahout-labelindex";
	        String dictionaryPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-vec/dictionary.file-0";
	
	        Configuration configuration = new Configuration();
	        
	        // 加载Mahout朴素贝叶斯分类模型，以及相关的类标、字典和分类名称映射
	        NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
	        classifier = new StandardNaiveBayesClassifier(model);
	        labels = BayesUtils.readLabelIndex(configuration, new Path(labelIndexPath));
	        dictionary = readDictionnary(configuration, new Path(dictionaryPath));
	        categoryMapping = loadCategoryMapping();
	        
	        // 加载新增的用户行为数据
	        term2category = loadTerm2Category("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/term_category_qc.txt");
	        
	        //使用IKAnalyzer进行中文分词
	        ikanalyzer = new IKAnalyzer();
	        
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	// 回收资源
	public void cleanup() {
		ikanalyzer.close();
	}
	
	public Map<String, Integer> readDictionnary(Configuration conf, Path dictionnaryPath) {
        Map<String, Integer> dictionnary = new HashMap<String, Integer>();
        for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
            dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
        }
        return dictionnary;
    }

    public Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
        Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
        for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
            documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
        }
        return documentFrequency;
    }
    
    public Map<String, String> loadCategoryMapping() {
    	
    	Map<String, String> categoryMapping = new HashMap<String, String>();
    	categoryMapping.put("1", "饼干");
    	categoryMapping.put("2", "方便面");
    	categoryMapping.put("3", "海鲜水产");
    	categoryMapping.put("4", "进口牛奶");
    	categoryMapping.put("5", "纯牛奶");
    	categoryMapping.put("6", "巧克力");
    	categoryMapping.put("7", "饮料饮品");
    	categoryMapping.put("8", "坚果");
    	categoryMapping.put("9", "食用油");
    	categoryMapping.put("10", "枣类");
    	categoryMapping.put("11", "新鲜水果");
    	categoryMapping.put("12", "大米");
    	categoryMapping.put("13", "面粉");
    	categoryMapping.put("14", "手机");
    	categoryMapping.put("15", "电脑");
    	categoryMapping.put("16", "美发护发");
    	categoryMapping.put("17", "沐浴露");
    	categoryMapping.put("18", "茶叶");
    	
    	return categoryMapping;
    	
    }
    
    public HashMap<String, HashMap<String, Double>> loadTerm2Category(String file) { 
    	
    	// 读取term_category_qc.txt，并将其加载到内存
    	// 如果数据量大，可能需要考虑通过数据库这样的持久化存储，来保存和读取用户行为数据
    	ObjectMapper mapper = new ObjectMapper();
    	HashMap<String, HashMap<String, Double>> term2category = new HashMap<String, HashMap<String, Double>>();
    	
    	try {
    		
    		BufferedReader br = new BufferedReader(new FileReader(file));
    		String strLine = null;
    		while ((strLine = br.readLine()) != null) {
    			String[] tokens = strLine.split("\t");
    			String term = tokens[0];
    			String json = tokens[1];
    			
    			JsonNode jnRoot = mapper.readValue(json, JsonNode.class);
    			if (jnRoot.size() > 0) {
    				HashMap<String, Double> category2prob = new HashMap<>();
    				Iterator<String> iter = jnRoot.fieldNames();
    				while (iter.hasNext()) {
    					String category = iter.next();
    					category2prob.put(category, jnRoot.get(category).asDouble());
    				}
    				term2category.put(term, category2prob);
    			}
    			
    		}
    		
    		br.close();
    		
    		
    	} catch (Exception e) {
			// TODO: handle exception
    		e.printStackTrace();
		}
    	
    	return term2category;
    }
    
    // 供搜索引擎适用的查询分类预测接口
    public Map<String, Double> predict(String query) {
    	
    	HashMap<String, Double> combinedClassification = new HashMap<>();
    	
    	try {
    	
	    	//进行中文分词，同时构造单词列表
	        Map<String, Integer> terms = new Hashtable<String, Integer>();
	        ts = ikanalyzer.tokenStream("myfield", query);
		    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
			ts.reset(); 
			while (ts.incrementToken()) {
			  if (term.length() > 0) {
				  String strTerm = term.toString();
				  Integer termId = dictionary.get(strTerm);
				  
				  if (termId != null) {
					  if (!terms.containsKey(strTerm)) {
						  terms.put(strTerm, 0);
					  }
					  terms.put(strTerm, terms.get(strTerm) + 1);
	//				  termsCnt ++;
				  }
			  }
			}
			ts.end();
			ts.close();
			
			//使用词频tf（term frequency）构造向量
			RandomAccessSparseVector rasvector = new RandomAccessSparseVector(100000);
	        for (Map.Entry<String, Integer> entry : terms.entrySet()) {
	            String strTerm = entry.getKey();
	            int tf = entry.getValue();
	            Integer termId = dictionary.get(strTerm);
	            rasvector.setQuick(termId, tf);
	        }
	        
	        //根据构造好的向量和之前训练的模型，进行分类
	        org.apache.mahout.math.Vector predictionVector = classifier.classifyFull(rasvector);
	        double sum = 0.0;
	        for(Element element : predictionVector.all()) {
	            double score = element.get();
	            sum += Math.pow(2.0, score);		// 用于归一化
	        }
	        
	        // 输出归一化后的、基于商品标题的分类结果
	        HashMap<String, Double> listingClassification = new HashMap<>();
	        for(Element element : predictionVector.all()) {
	            int categoryId = element.index();
	            String category = categoryMapping.get(labels.get(categoryId));
	            double score = element.get();
	            score = Math.pow(2.0, score) / sum;	//归一化
	            score = (int)(score * 100 + 0.5) / 100.0;
	            if (category != null) {
	            	listingClassification.put(category, score);
	            }
	        }
//	        System.out.println("基于商品标题的预测为：" + listingClassification);
	        
	        // 输出基于用户行为数据的分类结果
	        // 考虑到保留用户输入的原始语义，这里并不进行分词
	        HashMap<String, Double> behaviorClassification = term2category.get(query);
	        if (behaviorClassification != null) {
//	        	System.out.println("基于用户行为的预测为：" + behaviorClassification);
	        }
	        
	        
	        // 综合两种方式的最终分类结果
	        double bestScore = Double.MIN_VALUE;
	        String bestCategory = null;
	        for (String category : listingClassification.keySet()) {
	        	double behaviorWeight = 0.8;
	        	double listingWeight = 0.2;
	        	double behaviorScore = 0.0;
	        	 if (behaviorClassification != null) {
		        	if (behaviorClassification.containsKey(category)) {
		        		behaviorScore = behaviorClassification.get(category);
		        	}
	        	}
	        	double listingScore = listingClassification.get(category);
	        	double combinedScore = behaviorWeight * behaviorScore + listingWeight * listingScore;
	        	combinedScore = (int)(combinedScore * 100 + 0.5) / 100.0;
	        	
	        	if (combinedScore > bestScore) {
	        		
	        		bestScore = combinedScore;
	        		bestCategory = category;
	        		
	        	}
	        	
	        	combinedClassification.put(category, combinedScore);
	        }
//	        System.out.println("基于上述两者的预测为：" + combinedClassification);
	        
	        System.out.println(String.format("根据商品标题文本和用户行为，最终预测的分类为：%s", bestCategory));
//	        System.out.println();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
        
        return combinedClassification;
    	
    }

    public static void main(String[] args) throws Exception {
    	
    	NBQueryClassifierOnlineForSearch nbqcsearch = new NBQueryClassifierOnlineForSearch();
    	
        while (true) {
        	
        	BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
            System.out.print("请输入待测的文本：");  
            String content = strin.readLine();
            
            if ("exit".equalsIgnoreCase(content)) break;
            
            System.out.println(nbqcsearch.predict(content));
	        
        }
        
        
        nbqcsearch.cleanup();
    }

}
