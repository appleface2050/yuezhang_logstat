package com.zsmj.inputformat;

import java.util.Map;

/**
 * 掌阅日志记录实体
 * @author ZSMJ
 * @date 2012-03-21
 */
public class Record {

	//33个字段
	private String nodeid;
	private String inserttime;
	private String remoteip;
	private String productid;//pc默认值10
	private String featureid;
	private String behaviorid;
	private String actionid;
	private String useractiontype;//key,cg,club
	private String doffset;//ebk2_offset
	private String userid;//p1
	private String partnerid;//p2
	private String clientversionid;//p3
	private String runversionid;//p4
	private String screensize;//p5
	private String simid;//p6
	private String imeinumber;//p7
	private String smscenter;//p8
	private String simtype;//p9
	private String fontsize;//p10
	private String enkey;//p11
	private String searchbykeyword;//p
	private String searchbyauthor;//a
	private String usercustomizeid;//p15
	private String customerproject;//p16
	private String downprice;//pr,oid
	private String sechemeid;//s1
	private String productname;//p17
	private String compiletime;//p18
	private String productversion;//p19
	private String posmark;//pt
	private String zppname;//p12,zd1
	private String protocalid;//p0
	private String ordid;//oid
	private Map<String,String> data;
	
	public Record(Map<String,String> data){
		this.data = data;
	}
	
	public String getNodeid() {
		return data.get("nodeid");
	}

	public String getInserttime() {
		return data.get("inserttime");
	}

	public String getRemoteip() {
		return data.get("remoteip");
	}

	public String getProductid() {
		String pc = data.get("pc");
		if("".equals(pc)||pc==null){
			return "10";
		}else{
			return pc;//pc
		}
	}

	public String getFeatureid() {
		return data.get("featureid");
	}

	public String getBehaviorid() {
		if(data.containsKey("behaviorid")){
		    return data.get("behaviorid");
		}else{
			return "-999";
		}
	}

	public String getActionid() {
		String s = data.get("actionid");
		if("".equals(s)||null==s){
			if((s =getUseractiontype())!=null){
				String []item = s.split("_");
				if(item.length==3){
					return item[2];
				}else{
					return s;
				}
			}else{
				return "-999";
			}
		}else{
			return s;
		}
	}

	public String getUseractiontype() {
		if(data.containsKey("key")){
			return data.get("key");
		}else if(data.containsKey("cg")){
			return data.get("cg");
		}else if(data.containsKey("club")){
			return data.get("club");
		}else{
			return null;
		}
	}

	public String getDoffset() {
		String s = data.get("ebk2_offset");
		if("".equals(s)||null==s){
			return "-999";
		}else{
			return s;
		}
	}

	public String getUserid() {
		return data.get("p1");
	}

	public String getPartnerid() {
		String s = data.get("p2");
		if("".equals(s)||null==s){
			return "-999";
		}else{
			return s;
		}
	}

	public String getClientversionid() {
		return data.get("p3");
	}

	public String getRunversionid() {
		return data.get("p4");
	}

	public String getScreensize() {
		return data.get("p5");
	}

	public String getSimid() {
		String s = data.get("p6");
		if("".equals(s)||null==s){
			return "-999";
		}else{
			return s;
		}
	}

	public String getImeinumber() {
		return data.get("p7");
	}

	public String getSmscenter() {
		return data.get("p8");
	}

	public String getSimtype() {
		String s = data.get("p9");
		if("0".equals(s)||"1".equals(s)||"3".equals(s)){
			return s;
		}else{
			return "-999";
		}
	}

	public String getFontsize() {
		return data.get("p10");
	}

	public String getEnkey() {
		return data.get("p11");
	}

	public String getSearchbykeyword() {
		String s = data.get("p");
		if(null!=s){
			if(s.length()>140){
		        return s.substring(0,140);
			}else{
				return s;
			}
		}else{
			return null;
		}
	}

	public String getSearchbyauthor() {
		String s = data.get("a");
		if(null!=s){
			if(s.length()>40){
		        return s.substring(0,40);
			}else{
				return s;
			}
		}else{
			return null;
		}
	}

	public String getUsercustomizeid() {
		return data.get("p15");
	}

	public String getCustomerproject() {
		return data.get("p16");
	}

	public String getDownprice() {
	    try{
		    if(data.containsKey("pr")){
		    	Integer.parseInt(data.get("pr"));
			    return data.get("pr");
		    }else if(data.containsKey("oid")){
			    String buf = data.get("oid");
			    if("free_01".equals(buf)){
			    	return "0";
			    }else{
			    	int index = buf.indexOf('_');
				    int end = buf.substring(index+1).indexOf('_');
				    buf = buf.substring(index+1,end+index+1);
				    Integer.parseInt(buf);
				    return buf;
			    }
		    }else{
			    return "0";
		    }
		}catch(Throwable e){
			    return "0";
		}
	}

	public String getSechemeid() {
		try{
		String s = null;
		if(data.containsKey("s1")){
			Integer.parseInt(data.get("s1"));
			return data.get("s1");
		}else if((s =getUseractiontype())!=null){
			String []item = s.split("_");
			if(item.length==3){
				Integer.parseInt(item[1]);
				return item[1];
			}else{
				return "1";
			}
		}else{
			return "1";
		}
		}catch(Throwable e){return "1";}
	}

	public String getProductname() {
		return data.get("p17");
	}

	public String getCompiletime() {
		return data.get("p18");
	}

	public String getProductversion() {
		return data.get("p19");
//		if(null!=s){
//			if(s.contains("#")){
//				return s.replace("ebk2_offset=0","").replace("#","_");
//			}else if(s.contains("ireader_1.1.1.0")){
//				return "ireader_1.1.1.0";
//			}else if("ireader".equals(getProductname())&&!s.contains("ireader_")){
//				return "ireader_"+s.replace("ebk2_offset=0","");
//			}else if("palmreader_iphone".equals(s)){
//				return "palmreader_iphone_1.0.0";
//			}
//		}
//		if("21".equals(getSechemeid())&&"palmeread".equals(getProductname())){
//			return "palmreader_java_1.10.0";
//		}else if("zhangyue_small".equals(getProductname())){
//			return "palmreader_small_mtk_1.0.0";
//		}else if(null!=getProductname()&&getProductname().contains("zhangyue_ibook_240")){
//			return "palmreader_ibook_mtk_1.0.0";
//		}else if(null!=getProductname()&&getProductname().contains("zhangyue_240")){
//			return "palmreader_mtk_1.0.0";
//		}
	}

	public String getPosmark() {
		return data.get("pt");
	}

	public String getZppname() {
		if(data.containsKey("p12")){
			return data.get("p12");
		}else if(data.containsKey("zd1")){
			return data.get("zd1");
		}else{
			return null;
		}
	}

	public String getProtocalid() {
		return data.get("p0");
	}

	public String getOrderID(){
		return data.get("oid");
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append(this.getNodeid()).append('\t');
		buf.append(this.getInserttime()).append('\t');
		buf.append(getEffectiveLengthString(this.getRemoteip(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getProductid(),48)).append('\t');
		buf.append(getEffectiveLengthString(this.getFeatureid(),48)).append('\t');
		buf.append(this.getBehaviorid()).append('\t');
		buf.append(this.getActionid()).append('\t');
		buf.append(getEffectiveLengthString(this.getUseractiontype(),48)).append('\t');
		buf.append(getEffectiveLengthString(this.getDoffset(),14)).append('\t');
		buf.append(getEffectiveLengthString(this.getUserid(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getPartnerid(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getClientversionid(),9)).append('\t');
		buf.append(getEffectiveLengthString(this.getRunversionid(),14)).append('\t');
		buf.append(getEffectiveLengthString(this.getScreensize(),9)).append('\t');
		buf.append(getEffectiveLengthString(this.getSimid(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getImeinumber(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getSmscenter(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getSimtype(),2)).append('\t');
		buf.append(getEffectiveLengthString(this.getFontsize(),9)).append('\t');
		buf.append(getEffectiveLengthString(this.getEnkey(),24)).append('\t');
		buf.append(this.getSearchbykeyword()).append('\t');
		buf.append(this.getSearchbyauthor()).append('\t');
		buf.append(getEffectiveLengthString(this.getUsercustomizeid(),48)).append('\t');
		buf.append(getEffectiveLengthString(this.getCustomerproject(),60)).append('\t');
		buf.append(getEffectiveLengthString(this.getDownprice(),2)).append('\t');
		buf.append(getEffectiveLengthString(this.getSechemeid(),4)).append('\t');
		buf.append(getEffectiveLengthString(this.getProductname(),20)).append('\t');
		buf.append(getEffectiveLengthString(this.getCompiletime(),9)).append('\t');
		buf.append(getEffectiveLengthString(this.getProductversion(),30)).append('\t');
		buf.append(getEffectiveLengthString(this.getPosmark(),24)).append('\t');
		buf.append(getEffectiveLengthString(this.getZppname(),48)).append('\t');
		buf.append(getEffectiveLengthString(this.getProtocalid(),9)).append('\t');
		buf.append(getEffectiveLengthString(this.getOrderID(),48));
		return buf.toString();
	}
	
	public static String getEffectiveLengthString(String src, int effectiveLength) {
		if (src != null && src.length() > effectiveLength) {
			return src.substring(0, effectiveLength);
		} else {
			return src;
		}
	}
}
