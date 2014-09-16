package com.zsmj.inputformat;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

public class Email {

	public static final String DEFAULT_HOSTNAME = "smtp.zhangyue.com";
	public static final String DEFAULT_USERNAME = "";//"luweimin@zhangyue.com";
	public static final String DEFAULT_PASSWORD = "8204abcd";
	public static final String DEFAULT_RECEIVER = "";//"13718726719@139.com";
	public static final String DEFAULT_SENDER = "";//"luweimin@zhangyue.com";
	public static final String DEFAULT_FETION_RECEIVER = "";//"13718726719";
	public static final String OK_STATUS = "Send SMS OK";
	public static final String DEFAULT_FETION_SCRIPT = "";//"sh /home/hadoop/dataware/jobstartshell/fetion.sh";
	public static void send(String hostname, String userName, String password,
			String receiver, String sender, String charset, String subject,
			String msg) {
		SimpleEmail email = new SimpleEmail();
		email.setTLS(true);
		email.setHostName(hostname);
		email.setAuthentication(userName, password);

		try {
			email.setCharset(charset);
			email.addTo(receiver);
			email.setFrom(sender);
			email.setSubject(subject);
			email.setMsg(msg);
			email.send();
		} catch (EmailException e) {
			e.printStackTrace();
		}
	}

	public static void defaultSend(String charset,String subject,String msg) {
		String command  = DEFAULT_FETION_SCRIPT+" "+DEFAULT_FETION_RECEIVER+" '"+msg+"'";
		try {
			CommandResult result = ExecShellCommand.exec(command);
			System.out.println(command);
			System.out.println(result.getExitValue()+":"+result.getOutput());
			if(result.getExitValue()==-1){
				send(DEFAULT_HOSTNAME,DEFAULT_USERNAME,DEFAULT_PASSWORD,DEFAULT_RECEIVER,DEFAULT_SENDER,charset,subject,msg);
			}else if(result.getOutput().indexOf(OK_STATUS)==-1||result.getError()!=null){
				send(DEFAULT_HOSTNAME,DEFAULT_USERNAME,DEFAULT_PASSWORD,DEFAULT_RECEIVER,DEFAULT_SENDER,charset,subject,msg);
			}
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			send(DEFAULT_HOSTNAME,DEFAULT_USERNAME,DEFAULT_PASSWORD,DEFAULT_RECEIVER,DEFAULT_SENDER,charset,subject,msg);
		}
	}
}
