package com.dksou.fitting.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class DateUtils {

	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static DateFormat daystr = new SimpleDateFormat("yyyyMMdd");
	private static DateFormat daystr2 = new SimpleDateFormat("yyyy-MM-dd");
	private static DateFormat hourstr = new SimpleDateFormat("yyyyMMddHH");
	private static DateFormat mstr = new SimpleDateFormat("mm");
	private static DateFormat mstr1 = new SimpleDateFormat("HH:mm");
	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
	
	public static String getHourStr()
	{
	  return 	hourstr.format(new Date());
	}
	public static String  getDayStr()
	{
		return daystr.format(new Date());
	}
	public static String  getAfterNDayStr(long n,String date)
	{
		Date parse = null;
		try {
			parse = sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return daystr2.format(parse.getTime() + (n * 86400000));
	}
	public static String get5Min()
	{
		Date date=new   Date();
		String mistr=new String((Integer.parseInt(mstr.format(date))/5)*5+"");
		if(mistr.length()==1)
		{
			mistr="0"+mistr;
		}
		
		
	return	getHourStr()+mistr;
	}
	
	public static String getdateStr()
	{
		return df.format(new Date());
	}
	
	public static Date setDate(){
		Calendar calendar = Calendar.getInstance();
		calendar.set(2014, 0, 1);
		Date time = calendar.getTime();
		return calendar.getTime();
	}

//	//根据不同粒度获得不同时间
//	public static String gettargetStr(String inteval)
//	{
//		String intval="";
//		if(FileWatcher.INTEVAL_DAY.equals(inteval))
//		{
//			intval= getDayStr();
//		}else if(FileWatcher
//				.INTEVAL_HOUR.equals(inteval))
//		{
//			intval= getHourStr();
//		} else if (FileWatcher.INTEVAL_5MIN.equals(inteval)){
//			intval= get5Min();
//		}
//		return intval;
//
//	}
	 public static String getTime(long time1,long time2){
		 long time3 = time1 - time2;
		 long h = time3 / (1000 * 60 *60);
		 long m = time3 / (1000 * 60) - h * 60;
		 return h +" 小时 : " + m +" 分钟";
	 }
	public static void main(String[] args) {
		System.out.println("相差时间: " + DateUtils.getTime(DateUtils.setDate().getTime(), DateUtils.setDate().getTime() - ((1000 * 60 * 60 * 24 * 2) - (2000 * 60))));
	}
	  
}
