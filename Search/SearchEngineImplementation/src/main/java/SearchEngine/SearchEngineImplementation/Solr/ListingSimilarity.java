package SearchEngine.SearchEngineImplementation.Solr;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.BytesRef;


public class ListingSimilarity extends ClassicSimilarity {
	
	public float lengthNorm(FieldInvertState state) {
		
		return 1.0f;
	}

	public float queryNorm(float sumOfSquaredWeights) {
		return 1.0f;
	}

	public float tf(float freq) {
		return 1.0f;
	}

	public float sloppyFreq(int distance) {
		return 1.0f;
	}
	
	
	public float idf(long docFreq, long docCount) {
		return 1.0f;
	}

	
	public float coord(int overlap, int maxOverlap) {
		return 1.0f;
	}
	
	public float scorePayload(int doc, int start, int end, BytesRef payload) {
	    return 1;
	}
	
}
