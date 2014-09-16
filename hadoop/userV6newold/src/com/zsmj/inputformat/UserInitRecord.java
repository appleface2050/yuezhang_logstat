package com.zsmj.inputformat;

import java.util.Map;

/**
 * 掌阅日志记录实体
 * 
 * @author ZSMJ
 * @date 2012-03-21
 */
public class UserInitRecord {

	// p1,p2,p4,p7,user,p16,p19,init_time
	private String p1;
	private String p2;
	private String p4;
	private String p7;
	private String usr;
	private String p16;
	private String p19;
	private String init_time;

	private Map<String, String> data;

	public UserInitRecord(Map<String, String> data) {
		this.data = data;
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(this.getInit_time()).append('\t');
		buf.append(getEffectiveLengthString(this.getP1(), 24)).append('\t');
		buf.append(getEffectiveLengthString(this.getP2(), 24)).append('\t');

		buf.append(getEffectiveLengthString(this.getP4(), 14)).append('\t');

		buf.append(getEffectiveLengthString(this.getP7(), 24)).append('\t');

		buf.append(getEffectiveLengthString(this.getP16(), 60)).append('\t');

		buf.append(getEffectiveLengthString(this.getP19(), 30)).append('\t');

		return buf.toString();
	}

	public static String getEffectiveLengthString(String src,
			int effectiveLength) {
		if (src != null && src.length() > effectiveLength) {
			return src.substring(0, effectiveLength);
		} else {
			return src;
		}
	}

	public String getP1() {
		return data.get("p1");
	}

	public String getP2() {
		return data.get("p2");
	}

	public String getP4() {
		return data.get("p4");
	}

	public String getP7() {
		return data.get("p7");
	}

	public String getUsr() {
		return data.get("usr");
	}

	public String getP16() {
		return data.get("p16");
	}

	public String getP19() {
		return data.get("p19");
	}

	public String getInit_time() {
		return data.get("init_time");
	}

}
