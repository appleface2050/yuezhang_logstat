package com.zsmj.inputformat;

import java.net.URLDecoder;
import java.util.HashMap;

public class SplitParser {
	protected static class Elem {
		public String raw = null;
		public String name = null;
		public int limit = 0;
		public Elem(String raw, String name, int limit){
			this.raw = raw;
			this.name = name;
			this.limit = limit;
		}
	}
	protected static final Elem [] DownloadConf = {
		new Elem("p1","uid",45),             // p1-->系统启动事唯一分配的用户id
		new Elem("p2","partner_id",45),      // p2-->渠道id
		new Elem("p3","version_id",18),      // p3-->客户端版本id
		new Elem("p4","run_id",28),          // p4-->运行平台库版本
		new Elem("p5","lcd_id",8),           // p5-->lcd id屏幕大小
		new Elem("p6","icc_id",45),          // p6-->sim卡背后的20位唯一编码
		new Elem("p7","imei",45),            // p7-->手机imei号，15位
		new Elem("p8","sms_center",24),      // p8-->短信中心号码
		new Elem("p9","sim_type",4),         // p9-->sim卡类型
		new Elem("p10","font_size",8),       // p10-->当前使用字体大小
		new Elem("p15","product_number",95), // p15-->机器型号（数字）
		new Elem("p16","product_name",95),   // p16-->机器型号
		new Elem("p19","version_name",45),   // p19-->带版本号的应用名字
		new Elem("p21","net_type",4),        // p21-->上网类型
		new Elem("p22","os_version",18),     // p22-->操作系统版本号
		new Elem("p23","rom_version",95),    // p23-->系统ROM版本信息
		new Elem("usr","user_name",24),      // usr-->注册用户名
		new Elem("rgt","reg_type",8),        // rgt-->注册类型
		new Elem("bid","bid",24),            // bid-->书id
		new Elem("cid","cid",24),            // cid-->章节id
		new Elem("pk","p_key",24),           // pk--->url来源(parent key)
		new Elem("pr","price_code",4),       // pr--->计费类型
		new Elem("type","down_type",4),      // type->下载类型
		new Elem("fu","fee_unit",8),         // fu--->计费单元
		new Elem("price","price",4),         // price->计费价格
		new Elem("pc","op_id",4),            // pc->取值为10标识为书城，相对于杂志、动漫
		new Elem("ip","ip",28),              // 用户ip
		new Elem("datetime","date_time",24)  // 日志时间
	};
	protected static final Elem [] ServiceConf = {
		new Elem("p1","uid",45),             // p1-->系统启动事唯一分配的用户id
		new Elem("p2","partner_id",45),      // p2-->渠道id
		new Elem("p3","version_id",18),      // p3-->客户端版本id
		new Elem("p4","run_id",28),          // p4-->运行平台库版本
		new Elem("p5","lcd_id",8),           // p5-->lcd id屏幕大小
		new Elem("p6","icc_id",45),          // p6-->sim卡背后的20位唯一编码
		new Elem("p7","imei",45),            // p7-->手机imei号，15位
		new Elem("p8","sms_center",24),      // p8-->短信中心号码
		new Elem("p9","sim_type",4),         // p9-->sim卡类型
		new Elem("p10","font_size",8),       // p10-->当前使用字体大小
		new Elem("p15","product_number",95), // p15-->机器型号（数字）
		new Elem("p16","product_name",95),   // p16-->机器型号
		new Elem("p19","version_name",45),   // p19-->带版本号的应用名字
		new Elem("p21","net_type",4),        // p21-->上网类型
		new Elem("p22","os_version",18),     // p22-->操作系统版本号
		new Elem("p23","rom_version",95),    // p23-->系统ROM版本信息
		new Elem("usr","user_name",24),      // usr-->注册用户名
		new Elem("rgt","reg_type",8),        // rgt-->注册类型
		new Elem("key","key",24),            // key-->url key统计标识，例：17B
		new Elem("pk","p_key",24),           // pk--->url来源(parent key)
		new Elem(null,"feature_id",28),      // key-->key切分字母后数据，不包括分页E1
		new Elem("search","search",95),      // search-->搜索请求串
		new Elem("pg","page",50),            // pg或者 key参数E字母后的值-->请求页码
		new Elem("Act","act",50),            // action 或者 Act，优先取值Act-->url操作标识		
		new Elem("pc","op_id",24),           // pc--->取值为10标识为书城，相对于杂志、动漫
		new Elem("mcid","mcid",24),          // mcid->消息队列标识
		new Elem(null,"uri",95),             // url--->去除参数后的url链接, 例如：book.php
		new Elem("ip","ip",28),              // 用户ip
		new Elem("datetime","date_time",24)  // 日志时间
	};
	public static final String[] KEY_LIST = {"1R1","2R1","3R1","4R1","4R2","2R2","3R2",
		"4U1","4U2","4U3","4U4","4U5","4U6","4U7","4U8","4U9","4U10","4U11","4U12","4U13","4U14",
		"1U1","1U2","1U3","1U4","1U5","1U6","1U7","1U8","1U10","1U11","1U12","1U13","1U14","1U15",
		"2U1","2U2","2U3","3U1","3U2","5U","6U","7U1","7U2","8U","9U","10U","11U",
		"128B","126B","127B","4B4","4B100","4B101","4B102","4B104","1K1","1P1","17B","129B","6B","1K","11B","1W1","10B",
		"1S1","2S1","2S2",
		"4B6",
		};
	protected static String feature_id = null;
	protected static String page = null;
	
	// 解析query string --> key:value map
	public static HashMap<String,String> parseQs(String uri, String datetime, String ip){
		HashMap<String,String> map = new HashMap<String,String>();
		String[] arr = uri.split("\\?");
		if (arr.length >= 2) {
			String[] qarr = arr[1].split("&");
			for (int i = 0; i < qarr.length; i++) {
				String[] param = qarr[i].split("=");
				if (param.length >= 2){
					try{
						param[1] = URLDecoder.decode(param[1],"utf-8");
					}
					catch(Throwable e){}
					map.put(param[0],param[1]);
				}
			}
		}
		map.put("ip",ip);
		map.put("datetime",datetime);
		return map;
	}
	// 根据jobtype解析一条日志生成需要的中间结果
	public static String parse(String jobtype, String uri, String datetime, String ip) {
		Elem [] conf = null;
		if (jobtype.equals("download_v6"))
			conf = DownloadConf;
		else if (jobtype.equals("service_v6"))
			conf = ServiceConf;
		HashMap<String,String> pmap = SplitParser.parseQs(uri,datetime,ip);
		StringBuffer buf = new StringBuffer();
		for(int i=0;i<conf.length;i++) {
			Elem elem = conf[i];
			// process val
			String val = processValue(jobtype,uri,elem,pmap);			
			// unifying
			if (val != null && val.length() > elem.limit)
				val = val.substring(0,elem.limit);
			else if (val == null)
				val = "";
			// special p16取值为biztest为探测请求，不计入pv
			if (elem.raw!=null && elem.raw.equals("p16") && val.equals("biztest")){
				buf = null;
				break;
			}
			buf.append(val);
			if (i != conf.length-1)
				buf.append("\t");
		}
		return buf==null?null:buf.toString();
	}
	// 根据配置解析特定的字段的值，有些字段需要做一些特殊处理
	public static String processValue(String jobtype, String uri, Elem elem, HashMap<String,String> pmap) {
		String k = elem.raw;
		String name = elem.name;
		String val = pmap.get(k);
		
		if (name.equals("version_name")){
			if (val.equals("ireader_V2.0"))
				val = "ireader_2.0.0";
			else if (val.equals("ireader_V2.1"))
				val = "ireader_2.1.0";
			else if (val.startsWith("1.8.0.1ireader")){
				val = "ireader_1.8.0.1";
			}
		}
		
		if (jobtype.equals("download_v6")){
			if (k.equals("fu") && val == null)
				val = pmap.get("feeUnit");
			else if (k.equals("bid") || k.equals("price") || k.equals("p2"))
					if (!(Util.isNumeric(val) || Util.isDecimal(val)))
						val = null;
		}
		else if (jobtype.equals("service_v6")) {
			if (k == null){
				val = null;
				if (name.equals("feature_id"))
					val = SplitParser.feature_id;
				else if (name.equals("uri")){
					String [] arr = uri.split("\\?");
					if (arr.length > 0)
						val = arr[0];
				}
			}
			else if (k.equals("key")){ // format: {key}{featureid}E{page}
				String raw = val;
				for (int i = 0; i < KEY_LIST.length; i++) {
					if (raw != null && raw.startsWith(KEY_LIST[i])){
						val = KEY_LIST[i];
						String other = raw.substring(val.length());
						SplitParser.feature_id = other.replaceFirst("E\\d+$","");
						SplitParser.page = other.substring(SplitParser.feature_id.length()).replaceFirst("^E","");
						break;
					}
				}
				if (raw == val){
					SplitParser.feature_id = null;
					SplitParser.page = null;
				}
			}
			else if (k.equals("Act") && val == null)
				val = pmap.get("action");
			else if (k.equals("pg") && SplitParser.page != null)
				val = SplitParser.page;
		}
		return val;
	}
}
