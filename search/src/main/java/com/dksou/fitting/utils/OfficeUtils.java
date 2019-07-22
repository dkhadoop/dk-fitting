package com.dksou.fitting.utils;

import com.dksou.fitting.utils.FileEncode;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.SimpleTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.TextExtractionStrategy;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.POIXMLTextExtractor;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.xmlbeans.XmlException;

import java.io.*;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang.time.DateUtils.addDays;


/**
 * Created by Administrator on 2016/4/4 0004.
 */
public class OfficeUtils {
    public static String word2Text(File f) throws Exception {
        FileInputStream in = new FileInputStream (f);
        WordExtractor extractor = new WordExtractor(in);
        String str = extractor.getText();
        System.out.println("the result length is"+str.length());
        return str;
    }
    public static String jxlExcel2Txt(String filePath) throws Exception{
        String fileType = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
        InputStream stream = new FileInputStream(filePath);
        Workbook wb = null;
        if (fileType.equals("xls")) {
            wb = new HSSFWorkbook(stream);
        } else if (fileType.equals("xlsx")) {
            wb = new XSSFWorkbook(stream);
        } else {
            System.out.println("您输入的excel格式不正确");
        }
        Sheet sheet1 = wb.getSheetAt(0);
        for (Row row : sheet1) {
            for (Cell cell : row) {
                System.out.print(cell.getStringCellValue() + "  ");
            }
        }
        return null;
    }

    public static String poiExcel2Txt(String filePath) throws Exception {
        String code = FileEncode.getFileEncode(filePath);
        if("asci".equals(code)){
            // 这里采用GBK编码，而不用环境编码格式，因为环境默认编码不等于操作系统编码
            // code = System.getProperty("file.encoding");
            code = "GBK";
        }
        StringBuffer sb = new StringBuffer();
//        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(filePath)), "utf-8");
        Workbook workbook = WorkbookFactory.create(new FileInputStream(new File(filePath)));
        int numberOfSheets = workbook.getNumberOfSheets();
        for(int i = 0; i < numberOfSheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            int startRowNum = sheet.getFirstRowNum() + 1;
            int endRowNum = sheet.getLastRowNum();
            for (int rowNum = startRowNum; rowNum <= endRowNum; rowNum++) {
                Row row = sheet.getRow(rowNum);
                int startCellNum = 0;
                try {
                    startCellNum = row.getFirstCellNum();
                } catch (Exception e) {
                    continue;
                }
                int endCellNum = row.getLastCellNum() - 1;
                for (int cellNum = startCellNum; cellNum <= endCellNum; cellNum++) {
                    Cell cell = row.getCell(cellNum);  //

                    int type = 0;
                    try {
                        type = cell.getCellType();
                        //System.out.println( "type = [" + cell.getCellStyle().getDataFormat() + "]" );

                    } catch (Exception e) {
                        continue;
                    }

                    switch (type) {
                        case Cell.CELL_TYPE_NUMERIC://数值、日期类型
                            //System.out.println( "cell = [" + cell + "]" );
                        if (HSSFDateUtil.isCellDateFormatted(cell)) {//日期类型
                            //Date date = HSSFDateUtil.getJavaDate(d);
                            Date date = cell.getDateCellValue();
                            String format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).format( date );
                            sb.append(format).append("|");
                        }else if(cell.getCellStyle().getDataFormat()==177||cell.getCellStyle().getDataFormat()==176||cell.getCellStyle().getDataFormat()==178||cell.getCellStyle().getDataFormat()==180) {
                            Calendar c = new GregorianCalendar(1900,0,-1);
                            Date date1 = c.getTime();
                            //System.out.println(date1.toLocaleString());
                            Date _d = addDays(date1, (int) cell.getNumericCellValue() );  //42605是距离1900年1月1日的天数
                            //System.out.println(_d.toLocaleString());
                            sb.append(_d.toLocaleString()).append("|");
                        }else{//数值类型
                            sb.append((long)cell.getNumericCellValue()).append("|");
                            //System.out.println(" "+cell.getNumericCellValue() + "");
                        }
                            break;
                        case Cell.CELL_TYPE_BLANK://空白单元格
//                        System.out.print(" null ");
                            break;
                        case Cell.CELL_TYPE_STRING://字符类型
                            sb.append(cell.getStringCellValue()).append("|");
//                            System.out.print(" " + cell.getStringCellValue() + " ");
                            break;
                        case Cell.CELL_TYPE_BOOLEAN://布尔类型
                            sb.append(cell.getBooleanCellValue()).append("|");
//                            System.out.println(cell.getBooleanCellValue());
                            break;
                        default:
                            System.err.println("error");//未知类型
                            break;
                    }
                }
//                sb.append("/n");
            }
        }
        String res = new String(sb.toString().getBytes("utf-8"),"utf-8");
        return sb.toString();
    }

    public static  String poiExcel(String filePath) throws Exception {
        String code = FileEncode.getFileEncode(filePath);
        if("asci".equals(code)){
            // 这里采用GBK编码，而不用环境编码格式，因为环境默认编码不等于操作系统编码
            // code = System.getProperty("file.encoding");
            code = "GBK";
        }
        StringBuffer sb = new StringBuffer();
//        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(filePath)), "utf-8");
        Workbook workbook = WorkbookFactory.create(new FileInputStream(new File(filePath)));
        int numberOfSheets = workbook.getNumberOfSheets();
        return "";
    }
    /**
     * 获取内容标题的方法
     */
    public static List gettitles(String fname) throws Exception {
        String code = FileEncode.getFileEncode(fname);
        if("asci".equals(code)){
            // 这里采用GBK编码，而不用环境编码格式，因为环境默认编码不等于操作系统编码
            // code = System.getProperty("file.encoding");
            code = "GBK";
        }
        StringBuffer sb = new StringBuffer();
        Workbook workbook = WorkbookFactory.create(new FileInputStream(new File(fname)));
        Cell cell = null;
        List list=new ArrayList(  );
        try {
            Sheet sheetAt = workbook.getSheetAt( 1 );
            Row row = sheetAt.getRow( 1 );
            for (int cellnum = row.getFirstCellNum(); cellnum <row.getLastCellNum() ; cellnum++) {
                cell= row.getCell(cellnum );
                list.add( cell.getStringCellValue() );
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return list;
    }



//    public static  String tikaPdf2Txt(String filePath){
//        try{
//            BodyContentHandler handler = new BodyContentHandler(-1);
//            Metadata metadata = new Metadata();
//            FileInputStream inputstream = new FileInputStream(new File(filePath));
//            ParseContext pcontext = new ParseContext();
//
//
//            PDFParser pdfparser = new PDFParser();
//            pdfparser.parse(inputstream, handler, metadata,pcontext);
//
//
//            System.out.println("Contents of the PDF :" + handler.toString());
//
////打印文档属性，可删除
//            System.out.println("Metadata of the PDF:");
//            String[] metadataNames = metadata.names();
//
//            for(String name : metadataNames) {
//                System.out.println(name+ " : " + metadata.get(name));
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return null;
//    }

    public static String itextPdf2Txt(String filePath) throws Exception {
        PdfReader reader = new PdfReader(filePath);
        PdfReaderContentParser parser = new PdfReaderContentParser(reader);
        StringBuffer buff = new StringBuffer();
        TextExtractionStrategy strategy;
        for (int i = 1; i <= reader.getNumberOfPages(); i++) {
             strategy = parser.processContent(i,
                    new SimpleTextExtractionStrategy());
             buff.append(strategy.getResultantText());
          }
//        String res = new String(buff.toString().getBytes("utf-8"), "utf-8");
        return buff.toString();
    }



    public static String ParseDocOrDocx(String FilePath)
    {
        String ReturnStr = null;

        try{
            FilePath=URLDecoder.decode(FilePath,"utf-8");
        }catch (UnsupportedEncodingException e1){
            e1.printStackTrace();
        }
        String suffix = FilePath.substring(FilePath.lastIndexOf("."));

        try{
            if(".doc".equals(suffix)){ //97-03
                ReturnStr=Parse03(FilePath);
            }else if(".docx".equals(suffix)){ //2007
                ReturnStr=Parse07(FilePath);
            }else{
                System.out.println("不支持的文件类型！");
                ReturnStr=null;
            }
        }catch (Exception e){
            System.out.println("解析Doc文件出错！");
            e.printStackTrace();
        }
        return ReturnStr;
    }

    public static String Parse03(String FilePath) throws IOException{
        String text2003=null;
        InputStream is = null;
        try {
            is = new FileInputStream(new File(FilePath));
            WordExtractor ex = new WordExtractor(is);
            text2003 = ex.getText();

        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            is.close();
        }
        return text2003;
    }

    public static String Parse07(String FilePath) throws IOException, XmlException, OpenXML4JException{
        String text2007=null;
        try{
            OPCPackage opcPackage = POIXMLDocument.openPackage(FilePath);
            POIXMLTextExtractor extractor = new XWPFWordExtractor(opcPackage);
            text2007 = extractor.getText();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return text2007;
    }



    public static void main(String[] args) {
        try {
//            String content = word2Text(new File("C:\\Users\\Administrator\\Desktop\\大数据平台160401-莫永云.doc"));
//            String content = ParseDocOrDocx("C:\\Users\\Administrator\\Desktop\\aaa\\Sqoop测试结果.docx");
//            String content = ParseDocOrDocx("C:\\Users\\Administrator\\Desktop\\大数据平台160401-莫永云.doc");
//            System.out.println("content = " + content);
//            tikaPdf2Txt("C:\\Users\\Administrator\\Desktop\\dk_work\\设计文档\\交通\\Cloudera_5_x_部署手册.pdf");
            //String itextPdf2Txt = itextPdf2Txt("C:\\Users\\Administrator\\Desktop\\dk_work\\设计文档\\交通\\个性化推荐课程表-新.pdf");
            //System.out.println("itextPdf2Txt = " + itextPdf2Txt);
//            String jxlExcel2Txt = jxlExcel2Txt("C:\\Users\\Administrator\\Desktop\\dk_work\\设计文档\\交通\\FusionInsight HD V100R002C50 参数配置说明书 01.xlsx");
//            System.out.println("jxlExcel2Txt = " + jxlExcel2Txt);
//            String read = poiExcel2Txt("C:\\Users\\Administrator\\Desktop\\aaa\\FusionInsight HD V100R002C50 参数配置说明书 01.xlsx");
//            System.out.println("read = " + read);
//            String read = poiExcel2Txt("C:\\Users\\Administrator\\Desktop\\aaa\\大数据项目测试用例v0.5-151210.xls");
//            System.out.println("read = " + read);
            List gettitles = gettitles("E:\\fitting\\test数据\\excel文档test\\123123\\工作簿1.xlsx");
            for (int i = 0; i <gettitles.size() ; i++) {
                System.out.println( "title = [" + gettitles.get( i )+ "]" );
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
