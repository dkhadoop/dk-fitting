package com.dksou.fitting.dataprocess.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveUtil {

	public static Statement createStmt(String driverName, String url,
			String hostName, String hostPassword) throws Exception {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,
				hostPassword);
		Statement stmt = conn.createStatement();
		return stmt;
	}

	public static void createTab(Statement stmt, String tableName, int fdSum,
			String spStr, String dirName) throws Exception {
		// 表中字段
		StringBuffer fileds = new StringBuffer();

		for (int i = 1; i <= fdSum; i++) {
			if (fileds.length() == 0) {
				fileds.append("f" + i + " string");
			} else {
				fileds.append(",").append("f" + i + " string");
			}

		}

		// 创建表
		String sql = "create external table " + tableName + " (" + fileds
				+ ") " + "row format delimited fields terminated by '" + spStr
				+ "'" + " location " + "'" + dirName + "'";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static long selectTab(Statement stmt, String tableName, String fun,
			int fdNum) throws Exception {
		String sql = "select " + fun + "(f" + fdNum + ") from " + tableName;
		System.out.println(sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			// System.out.println(res.getInt(1));
			return res.getLong(1);
		}
		return 0;
	}

	public static void deleteTab(Statement stmt, String tableName) throws Exception {
		String sql = "drop table if exists " + tableName;
		stmt.execute(sql);		
	}

	public static long selectTab(Statement stmt, String tableName, String fun,
			int fdNum, String compStr, String whereStr) throws Exception {
		String sql = "select " + fun + "(f" + fdNum + ") from " + tableName+" where f"+ fdNum +" "+compStr+" "+whereStr;
		System.out.println(sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			// System.out.println(res.getInt(1));
			return res.getLong(1);
		}
		return 0;
	}

	public static void createTab(Statement stmt, String tableName,
			String srcDirName) throws Exception {
		// create table tableName (f1 string) location srcDirName
		String sql = "create external table " + tableName + " (f string) " + " location " + "'" + srcDirName + "'";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void formatRec(Statement stmt, String dstDirName, String srcTable,
			String dstTable, String spStr,  int fdSum) throws Exception {
		// create table t4 location "/zxzy" as select f1 from t1 where size(split(f1,","))=24;
		String sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where size(split(f,"  + "'"+spStr+"')) = "+fdSum;
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void toExTable(Statement stmt, String dstTable) throws Exception {
		// ALTER TABLE t4 SET TBLPROPERTIES ('EXTERNAL'='TRUE');
		String sql = "ALTER TABLE "+dstTable+" SET TBLPROPERTIES ('EXTERNAL'='TRUE')";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void formatField(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String fdNum, String regExStr) throws Exception {
		String sql="";
		// select f from t1 where not split(f1,",")[1] like "%a%" and not split(f1,",")[2] like "%a%"
		if (fdNum.trim().toString().equals("0")) {
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where not f like '%" + regExStr +"%'";
		}else if (fdNum.trim().split(",").length==1) {
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fdNum.trim())-1) +"] like '%" + regExStr +"%'";
		}else if (fdNum.trim().split(",").length>1) {
			String str="";			
			for (int i =0;i<fdNum.trim().split(",").length;i++) {
				String[] fds = fdNum.trim().split(",");
				String[] regs = regExStr.trim().split(",");
				if (i==0) {
					str="not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fds[i])-1) +"] like '%" + regs[i] +"%'";
				}else {
					str=str+" and not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fds[i])-1) +"] like '%" + regs[i] +"%'";
				}
			}
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where " + str;
		}
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void selectField(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String fdNum) throws Exception {
		// create table t4 location "/zxzy" as select f1,f2,f3 from t1;
		
		String str="";
		for (String fd : fdNum.trim().split(",")) {
			if (str=="") {
				str="f"+ fd.trim();
			}else {
				str=str+" ,"+"f"+ fd.trim();
			}
		}
		
		String sql = "create table " + dstTable + " row format delimited fields terminated by '" + spStr
				+ "'"  +" location " + "'" + dstDirName + "' as select "+str+" from "+srcTable;
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void selectRec(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String whereStr) throws Exception {
		// TODO Auto-generated method stub
		String sql = "create table " + dstTable + " row format delimited fields terminated by '" + spStr
				+ "'" +" location " + "'" + dstDirName + "' as select * from "+srcTable +" where "+whereStr;
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void dedupe(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, String fdNum) throws Exception {
		// TODO Auto-generated method stub
		String sql="";
		// select distinct f from t1
		if (fdNum.trim().toString().equals("0")) {
			sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as select distinct f from "+srcTable;
		}else if (fdNum.trim().split(",").length==1) {
			
			sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as select distinct split(f,'"+spStr+"')["+(Integer.parseInt(fdNum.trim())-1)+"] from "+srcTable;
		}else if (fdNum.trim().split(",").length>1) {
			String str="";			
			for (int i =0;i<fdNum.trim().split(",").length;i++) {
				String[] fds = fdNum.trim().split(",");

				if (i==0) {
					str="distinct split(f,'"+spStr+"')["+(Integer.parseInt(fds[i])-1)+"]";
				}else {
					str=str+" , split(f,'"+spStr+"')["+(Integer.parseInt(fds[i])-1)+"]";
				}
			}
			sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as select "+ str +" from "+srcTable;
		}
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void analyse2(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr, int fdSum,
			String pNum, String oNum, String whereStr) throws Exception {
		
		 String sql ="create table "+midTable+" as select "+oNum +" as id, " +" ROW_NUMBER() OVER (PARTITION by "+oNum+") as rn, "+pNum +" as pid from "+srcTable
				 +" where "+whereStr;
		 System.out.println(sql);
		 stmt.execute(sql);
		 
		 sql = "create table " + dstTable +" row format delimited fields terminated by '\\t' COLLECTION ITEMS TERMINATED BY ',' location " + "'" + dstDirName + "' as " +
				  "select pid2,round(m.num/n.total,4) as support from " +
				  " (select pid2,count(*) num from " +
				  " ( select a.id,sort_array(array(a.pid,b.pid)) as pid2" +
				  " from" +
				  " (select id,rn,pid from "+midTable+")a" +
				  " join " +
				  " (select id,rn,pid from "+midTable+")b" +
				  " on a.id=b.id" +
				  " where a.rn<b.rn" +
				  " )t" +
				  " group by pid2" +
				  " )m" +
				  " left outer join " +
				  " (" +
				  " select count(distinct id) as total from "+midTable+
				  " )n" +
				  " on 1=1" +
				  " where " +
				  " m.num/n.total>=0.4";
		 System.out.println(sql);
		 stmt.execute(sql);
		 
	}

	public static void analyse3(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr,
			int fdSum, String pNum, String oNum, String whereStr) throws Exception {
		 String sql ="create table "+midTable+" as select "+oNum +" as id, " +" ROW_NUMBER() OVER (PARTITION by "+oNum+") as rn, "+pNum +" as pid from "+srcTable
				 +" where "+whereStr;
		 System.out.println(sql);
		 stmt.execute(sql);
		 
		 sql = "create table " + dstTable +" row format delimited fields terminated by '\\t' COLLECTION ITEMS TERMINATED BY ',' location " + "'" + dstDirName + "' as " +
				  "select pid2,round(m.num/n.total,4) as support from " +
				  " (select pid2,count(*) num from " +
				  " ( select a.id,sort_array(array(a.pid,b.pid,c.pid)) as pid2" +
				  " from" +
				  " (select id,rn,pid from "+midTable+")a" +
				  " join " +
				  " (select id,rn,pid from "+midTable+")b" +
				  " on a.id=b.id" +
				  " join " +
				  " (select id,rn,pid from "+midTable+")c" +
				  " on b.id=c.id" +
				  " where a.rn<b.rn and b.rn<c.rn" +
				  " )t" +
				  " group by pid2" +
				  " )m" +
				  " left outer join " +
				  " (" +
				  " select count(distinct id) as total from "+midTable+
				  " )n" +
				  " on 1=1" +
				  " where " +
				  " m.num/n.total>=0.2";
		 System.out.println(sql);
		 stmt.execute(sql);
		 
	}

	public static void groupAnalyse(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr,
			int fdSum, String whereStr, String groupStr) throws Exception {
		String sql;
		if(groupStr.compareTo("1")==0){
			 sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as select count(*) from "+srcTable
					 +" where "+whereStr+" group by "+groupStr;
		}else {
			sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as select "+groupStr+", count(*) from "+srcTable
					 +" where "+whereStr+" group by "+groupStr;
		}
		 System.out.println(sql);
		 stmt.execute(sql);
		 
	}
	
	//频繁二项集改进
	public static void apriori2(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr, int fdSum,
			String pNum, String oNum, String whereStr) throws Exception {
		
		 String sql ="create table "+midTable+" as select "+oNum +" as id, " +" ROW_NUMBER() OVER (PARTITION by "+oNum+") as rn, "+pNum +" as pid from "+srcTable
				 +" where "+whereStr;
		 System.out.println(sql);
		 stmt.execute(sql);
		 		 
		 sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as " +
				 "select" +
			      " pid_a, pid_b, round(ab_num/n.total,4) as support, round(ab_num/produce_num.num,4) as confidence" +
			      " from" +
			      "  (" +
			      "    select pid_a, pid_b, count(*) ab_num" +
			      "    from " +
			      "     (" +
			      "      select a.id, a.pid pid_a, b.pid as pid_b" +
			      "       from" +
			      "       (select id,rn,pid from "+midTable+")a" +
			      "        join " +
			      "       (select id,rn,pid from "+midTable+")b" +
			      "        on a.id=b.id" +
			      "        where a.rn<>b.rn" +
			      "      )t" +
			      "     group by pid_a, pid_b" +
			      "   )m" +
			      "  left outer join (select "+pNum+" as pid,count(distinct "+oNum+") as num from "+srcTable+" group by "+pNum+") produce_num" +
			      "   on m.pid_a=produce_num.pid" +
			      "  left outer join " +
			      "  (" +
			      "    select count(distinct id) as total from "+midTable+
			      "   )n" +
			      " on 1=1" +
			      " where " +
			      " ab_num/n.total>=0.4";
		 System.out.println(sql);
		 stmt.execute(sql);
		 
	}
	
	
	//频繁三项集改进
	public static void apriori3(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr, int fdSum,
			String pNum, String oNum, String whereStr) throws Exception {
		
		 String sql ="create table "+midTable+" as select "+oNum +" as id, " +" ROW_NUMBER() OVER (PARTITION by "+oNum+") as rn, "+pNum +" as pid from "+srcTable
				 +" where "+whereStr;
		 System.out.println(sql);
		 stmt.execute(sql);
		 		 
		 sql = "create table " + dstTable +" row format delimited fields terminated by '"+spStr+"' location " + "'" + dstDirName + "' as " +
				 "select" +
			      " pid_a, pid_b, pid_c, round(ab_num/n.total,4) as support, round(ab_num/produce_num.num,4) as confidence" +
			      " from" +
			      "  (" +
			      "    select pid_a, pid_b, pid_c, count(*) ab_num" +
			      "    from " +
			      "     (" +
			      "      select a.id, a.pid pid_a, b.pid as pid_b, c.pid as pid_c" +
			      "       from" +
			      "       (select id,rn,pid from "+midTable+")a" +
			      "        join " +
			      "       (select id,rn,pid from "+midTable+")b" +
			      "        on a.id=b.id" +
			      "        join " +
				  "       (select id,rn,pid from "+midTable+")c" +
				  "        on b.id=c.id" +
				  "        where a.rn<>b.rn and b.rn<>c.rn and a.rn<>c.rn" +
			      "      )t" +
			      "     group by pid_a, pid_b, pid_c" +
			      "   )m" +
			      "  left outer join (select "+pNum+" as pid,count(distinct "+oNum+") as num from "+srcTable+" group by "+pNum+") produce_num" +
			      "   on m.pid_a=produce_num.pid" +
			      "  left outer join " +
			      "  (" +
			      "    select count(distinct id) as total from "+midTable+
			      "   )n" +
			      " on 1=1" +
			      " where " +
			      " ab_num/n.total>=0.2";
		 System.out.println(sql);
		 stmt.execute(sql);
		 
	}

	//基于频繁二项集的预测
	public static void predict2(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr,
			int fdSum, String pNum, String oNum, String whereStr, String hadItem) throws Exception {
		apriori2(stmt, dstDirName, srcTable, midTable, dstTable, spStr, fdSum, pNum, oNum, whereStr);
		//select pid_b from supp_conf where pid_a='I3'; 
		String sql = "select pid_b from "+dstTable+" where pid_a='"+hadItem+"'";
		System.out.println(sql);
		//stmt.execute(sql);
		ResultSet res = stmt.executeQuery(sql);
		System.out.println(hadItem+"经常出现的项集为：");
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	public static void predict3(Statement stmt, String dstDirName,
			String srcTable, String midTable, String dstTable, String spStr,
			int fdSum, String pNum, String oNum, String whereStr, String hadItem) throws Exception {
		apriori3(stmt, dstDirName, srcTable, midTable, dstTable, spStr, fdSum, pNum, oNum, whereStr);
		//select pid_b from supp_conf where pid_a='I3'; 
		String sql = "select pid_b, pid_c from "+dstTable+" where pid_a='"+hadItem+"'";
		System.out.println(sql);
		//stmt.execute(sql);
		ResultSet res = stmt.executeQuery(sql);
		System.out.println(hadItem+"经常出现的项集为：");
		while (res.next()) {
			System.out.println(res.getString(1)+","+res.getString(2));
		}
		
	}
	
	
	
}
