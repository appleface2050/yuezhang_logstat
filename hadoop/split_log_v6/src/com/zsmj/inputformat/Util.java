package com.zsmj.inputformat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class Util {
	public static Date strptime(String date, String format) throws ParseException {
		if (date == null) {
			return null;
		}
		SimpleDateFormat fmt = new SimpleDateFormat(format);
		return fmt.parse(date);
	}
	
	public static String strftime(Date date, String format) {
		if (date == null) {
			return null;
		}
		return new SimpleDateFormat(format).format(date);
	}
	
	public static boolean isNumeric(String str){
		if (null == str)
			return false;
		return Pattern.compile("^\\d+$|-\\d+$").matcher(str).matches();
	 }
	
	public static boolean isDecimal(String str){
		if (null == str)
			return false;
		return Pattern.compile("\\d+\\.\\d+$|-\\d+\\.\\d+$").matcher(str).matches();
	 }
}
