package MahoutMachineLearning.Classification;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
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


public class NBClassifierOnline {
	
	public static Map<String, Integer> readDictionnary(Configuration conf, Path dictionnaryPath) {
        Map<String, Integer> dictionnary = new HashMap<String, Integer>();
        for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
            dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
        }
        return dictionnary;
    }

    public static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
        Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
        for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
            documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
        }
        return documentFrequency;
    }
    
    public static Map<String, String> loadCategoryMapping() {
    	
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

    public static void main(String[] args) throws Exception {
    	
    	//指定Mahout朴素贝叶斯分类模型的目录、类标文件和字典文件
        String modelPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-model/";
        String labelIndexPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-mahout-labelindex";
        String dictionaryPath = "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-vec/dictionary.file-0";

        Configuration configuration = new Configuration();
        
        // 加载Mahout朴素贝叶斯分类模型，以及相关的类标、字典和分类名称映射
        NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
        StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);
        Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration, new Path(labelIndexPath));
        Map<String, Integer> dictionary = readDictionnary(configuration, new Path(dictionaryPath));
        Map<String, String> categoryMapping = loadCategoryMapping();

        //使用IKAnalyzer进行中文分词
        Analyzer ikanalyzer = new IKAnalyzer();
        TokenStream ts = null;
        
        while (true) {
        	
        	BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
            System.out.print("请输入待测的文本：");  
            String content = strin.readLine();
            
            if ("exit".equalsIgnoreCase(content)) break;
            
	        //进行中文分词，同时构造单词列表
	        Map<String, Integer> terms = new Hashtable<String, Integer>();
	        ts = ikanalyzer.tokenStream("myfield", content);
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
	        double bestScore = -Double.MAX_VALUE;
	        int bestCategoryId = -1;
	        for(Element element : predictionVector.all()) {
	            int categoryId = element.index();
	            double score = element.get();
	            if (score > bestScore) {
	                bestScore = score;
	                bestCategoryId = categoryId;
	            }
	        }
	        System.out.println();
	        String category = categoryMapping.get(labels.get(bestCategoryId));
	        if (category == null) category = "未知";
	        System.out.println(String.format("预测的分类为：%s", category));
	        System.out.println();
	        
        }
        
        ikanalyzer.close();
    }

}
