package com.dksou.fitting.utils;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/** 关于poi.jar对execl的简单解析
 * @author trsli
 * 该类目前只支持对简单ececl的解析，具体操作根据个人要求设计。
 * 我要做的主要是通过某应用程序导出或者倒入execl文件，
 * 该类有部分方法属于网络借鉴，再加上个人业务需要改编。
 * */
public class PoiUtil {
    public PoiUtil() {
        // TODO Auto-generated constructor stub
    }

    private Workbook workbook;

    public PoiUtil(File execlFile) throws IOException, FileNotFoundException {
        workbook = new HSSFWorkbook( new FileInputStream( execlFile ) );
    }

    /**
     * 获取表中所有数据
     */
    public List<List> getDataSheet(int sheetNumber)
            throws FileNotFoundException {
        Sheet sheet = workbook.getSheetAt( sheetNumber );
        List<List> result = new ArrayList<List>();
        // 获取数据总行数，编号是从0开始的
        int rowcount = sheet.getLastRowNum() + 1;
        if (rowcount < 1) {
            return result;
        }
        // 逐行读取数据
        for (int i = 0; i < rowcount; i++) {
            // 获取行对象
            Row row = sheet.getRow( i );
            if (row != null) {
                List<Object> rowData = new ArrayList<Object>();
                // 获取本行中单元格个数
                int column = row.getLastCellNum();
                // 获取本行中各单元格的数据
                for (int cindex = 0; cindex < column; cindex++) {
                    Cell cell = row.getCell( cindex );
                    // 获得指定单元格中的数据
                     //getCellString( cell );
                    rowData.add( cell.getStringCellValue() );
                }
                result.add( rowData );
            }
        }
        return result;
    }

    /**
     * 获取单元格中的内容 ,该犯法用于解析各种形式的数据
     */
    private Object getCellString(HSSFCell cell) {
        Object result = null;
        if (cell != null) {
            int cellType = cell.getCellType();
            switch (cellType) {
                case HSSFCell.CELL_TYPE_STRING:
                    result = cell.getRichStringCellValue().getString();
                    break;
                case HSSFCell.CELL_TYPE_NUMERIC:
                    result = cell.getNumericCellValue();
                    break;
                case HSSFCell.CELL_TYPE_FORMULA:
                    result = cell.getNumericCellValue();
                    break;
                case HSSFCell.CELL_TYPE_ERROR:
                    result = null;
                    break;
                case HSSFCell.CELL_TYPE_BOOLEAN:
                    result = cell.getBooleanCellValue();
                    break;
                case HSSFCell.CELL_TYPE_BLANK:
                    result = null;
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    /**
     * 导出函数有三个参数，主内容数据，标题数组，到处文件名
     */
    public void createxls(Object[][] messages, String[] titles, String filename) {
        try {
            FileOutputStream fos = new FileOutputStream( new File( filename ) );
            HSSFWorkbook book = new HSSFWorkbook();// 所有execl的父节点
            HSSFSheet sheet = book.createSheet( "sheet1" );//此处可以随意设置
            HSSFRow hssfrow = sheet.createRow( 0 );//创建首行标题
            HSSFCell hssfcell = null;
            for (int i = 0; i < titles.length; i++) {//创建标题栏目，也就是表格第一行
                hssfcell = hssfrow.createCell( i );
                hssfcell.setCellType( HSSFCell.ENCODING_UTF_16 );
                hssfcell.setCellValue( titles[i] );
            }

            System.out.println( "message:" + messages.length );
            for (int i = 0; i < messages.length; i++) {//添加表格中的内容
                hssfrow = sheet.createRow( i + 1 );//创建表格第二行，由于标记为0,这里设置为一，主要为了区别标题和内容
                Object[] obj = messages[i];
                for (int j = 0; j < obj.length; j++) {
                    hssfcell = hssfrow.createCell( j );
                    hssfcell.setCellType( HSSFCell.ENCODING_UTF_16 );//关于数据编码的问题
                    hssfcell.setCellValue( obj[j] + "" );//转换为字符串的方式
                    System.out.print( obj[j] + "\t" );
                }
                System.out.println();
            }
            book.write( fos );
            fos.flush();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println( e );
        }
    }

    /**
     * 获取内容标题的方法
     */
    public String[] gettitles(String fname) {
        try {
            PoiUtil poi = new PoiUtil( new File( fname ) );
            List<List> datas = poi.getDataSheet( 0 );
            String[] title = new String[datas.get( 0 ).size()];//由于存储标题数据
            for (int i = 0; i < title.length; i++) {
                title[i] = datas.get( 0 ).get( i ).toString();//第一行
            }
            return title;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取数据内容的方法
     */
    public Object[][] getmessage(String fname) {
        try {
            PoiUtil poi = new PoiUtil( new File( fname ) );
            List<List> datas = poi.getDataSheet( 0 );
            Object[][] p = new Object[datas.size() - 1][datas.get( 0 ).size()];//二维数组大小，即用户存储表格内容数据
            for (int i = 1; i < datas.size(); i++) {
                List row = datas.get( i );
                for (int j = 0; j < row.size(); j++) {
                    Object value = row.get( j );
                    p[i - 1][j] = String.valueOf( value );
                }
            }
            return p;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println( e );
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        PoiUtil poi = new PoiUtil();
        //Object[][] p = poi.getmessage( "C:\\Users\\ASUS\\Desktop\\工作簿1.xlsx" );//倒入数据方法
        String[] title = poi.gettitles( "C:\\Users\\ASUS\\Desktop\\工作簿1.xls" );
        System.out.println( title.length + ":" + title[0] + title[1] );
        //System.out.println( p.length + "" + p[0][0] );//这是正确的
        //poi.createxls( p, title, "E:\\testli.xls" );//new String[]{"序号","姓名","成绩"}

    }
}
