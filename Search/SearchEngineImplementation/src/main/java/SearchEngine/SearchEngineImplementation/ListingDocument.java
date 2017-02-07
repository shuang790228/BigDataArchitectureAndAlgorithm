package SearchEngine.SearchEngineImplementation;

public class ListingDocument {
	
	// 必须的基础信息
	private long listing_id;
	private String listing_title;
	private long category_id;
	private String category_name;
	
	// 以下是可选的动态信息
	private String promotion_info = null;
	private String promotion_startdate = null;
	private String promotion_enddate = null;
	private double group_discount = -1.0;
	private String group_startdate = null;
	private String group_enddate = null;
	
	
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
	
	public String getPromotion_info() {
		return promotion_info;
	}

	public void setPromotion_info(String promotion_info) {
		this.promotion_info = promotion_info;
	}

	public String getPromotion_startdate() {
		return promotion_startdate;
	}

	public void setPromotion_startdate(String promotion_startdate) {
		this.promotion_startdate = promotion_startdate;
	}

	public String getPromotion_enddate() {
		return promotion_enddate;
	}

	public void setPromotion_enddate(String promotion_enddate) {
		this.promotion_enddate = promotion_enddate;
	}

	public double getGroup_discount() {
		return group_discount;
	}

	public void setGroup_discount(double group_discount) {
		this.group_discount = group_discount;
	}

	public String getGroup_startdate() {
		return group_startdate;
	}

	public void setGroup_startdate(String group_startdate) {
		this.group_startdate = group_startdate;
	}

	public String getGroup_enddate() {
		return group_enddate;
	}

	public void setGroup_enddate(String group_enddate) {
		this.group_enddate = group_enddate;
	}

	public String toString() {
		try {
			
			// 附加可选的动态信息，例如促销、团购等等
			StringBuffer sbDynamic = new StringBuffer();
			if (promotion_info != null) {
				sbDynamic.append(String.format("%s:%s%s ", promotion_info, promotion_startdate, promotion_enddate));
			}
			if (group_discount != -1.0) {
				sbDynamic.append(String.format("%s:%s%s ", group_discount, group_startdate, group_enddate));
			}
			
			return String.format("ListingDocument [listing_id = %d, listing_title = %s, category_id = %d, category_name = %s, dynamic_info = %s]", 
					listing_id, listing_title, category_id, category_name, sbDynamic);
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}
	
	
	
	
	

}
