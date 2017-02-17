package Preprocess;


public class Pair implements Comparable {
	
	public String strToken = null;
	public double dWeight = 0.0;
	private int descend = 1;
	

	public Pair() {
		
	}
	
	
	public Pair(String str, double iW, boolean bIsDescend) {
		strToken = str;
		dWeight = iW;
		if (!bIsDescend)  descend = -1;
	}

	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		
		if (((Pair) arg0).dWeight > dWeight) return descend;
		else if (((Pair) arg0).dWeight < dWeight) return (0 - descend);
		else return 0;
	}

}