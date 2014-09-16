package com.zsmj.inputformat.model;

public class UserBasicV6 {
	private String uid;
	private String partner_id;
	private String run_id;
	private String imei;
	private String user_name;
	private String product_name;
	private String version_name;
	private String first_init_time;
	
	private String last_init_time;
	private String first_vis_time;
	private String last_vis_time;
	
	public String getUid() {
		return uid;
	}
	public String getPartner_id() {
		return partner_id;
	}
	public String getRun_id() {
		return run_id;
	}
	public String getImei() {
		return imei;
	}
	public String getUser_name() {
		return user_name;
	}
	public String getProduct_name() {
		return product_name;
	}
	public String getVersion_name() {
		return version_name;
	}
	public String getFirst_init_time() {
		return first_init_time;
	}
	public String getLast_init_time() {
		return last_init_time;
	}
	public String getFirst_vis_time() {
		return first_vis_time;
	}
	public String getLast_vis_time() {
		return last_vis_time;
	}
	public UserBasicV6(String uid, String partner_id, String run_id,
			String imei, String user_name, String product_name,
			String version_name, String first_init_time, String last_init_time,
			String first_vis_time, String last_vis_time) {
		super();
		this.uid = uid;
		this.partner_id = partner_id;
		this.run_id = run_id;
		this.imei = imei;
		this.user_name = user_name;
		this.product_name = product_name;
		this.version_name = version_name;
		this.first_init_time = first_init_time;
		this.last_init_time = last_init_time;
		this.first_vis_time = first_vis_time;
		this.last_vis_time = last_vis_time;
	}	
}
