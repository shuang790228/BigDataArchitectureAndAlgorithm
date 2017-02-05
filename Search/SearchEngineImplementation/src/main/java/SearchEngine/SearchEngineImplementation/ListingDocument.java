package SearchEngine.SearchEngineImplementation;

public class ListingDocument {
	
	private long listing_id;
	private String listing_title;
	private long category_id;
	private String category_name;
	
	public ListingDocument() {
		
	}
	
	public ListingDocument(long listing_id, String listing_title, long category_id, String category_name) {
		this.listing_id = listing_id;
		this.listing_title = listing_title;
		this.category_id = category_id;
		this.category_name = category_name;
	}
	
	public long getListing_id() {
		return listing_id;
	}

	public void setListing_id(long listing_id) {
		this.listing_id = listing_id;
	}

	public String getListing_title() {
		return listing_title;
	}

	public void setListing_title(String listing_title) {
		this.listing_title = listing_title;
	}

	public long getCategory_id() {
		return category_id;
	}

	public void setCategory_id(long category_id) {
		this.category_id = category_id;
	}

	public String getCategory_name() {
		return category_name;
	}

	public void setCategory_name(String category_name) {
		this.category_name = category_name;
	}

	public String toString() {
		try {
			return String.format("ListingDocument [listing_id = %s, listing_title = %s, ]", listing_id, listing_title, category_id, category_name);
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}
	
	
	
	
	

}
