package com.dksou.fitting.utils;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

public class ExportExcelUtils {


	/**
	 * @param title：excel文件名称
	 * @param headers：表头
	 * @param dataset：要保存到表格的对象集合
	 * @param attrList：对象要保存的属性
	 * @param out：输出目的地
	 */
	public static void exportExcel(String title, String[] headers, Collection dataset,String attrList[], OutputStream out) {
		// 声明一个工作薄
		Workbook workbook = new HSSFWorkbook();
		// 生成一个表格
		Sheet sheet = workbook.createSheet(title);
		// 设置表格默认列宽度为15
		sheet.setDefaultColumnWidth((short) 15);
		// 产生表头
		Row row = sheet.createRow(0);
		//插入表头数据
		for (short i = 0; i < headers.length; i++) {
			Cell cell = row.createCell(i);
			//设置样式
			cell.setCellStyle(createHeaderStyle(workbook));
			HSSFRichTextString text = new HSSFRichTextString(headers[i]);
			cell.setCellValue(text);
		}
		int index = 1;
		for(Object bean : dataset){
			//得到一个bean，则生成表格的一行
			row = sheet.createRow(index++);
			for(int i=0;i<attrList.length;i++){
				PropertyDescriptor pd = null;
				try{
					pd = new PropertyDescriptor(attrList[i],bean.getClass());
				}catch (Exception e) {
					throw new RuntimeException("bean中没有属性：" + attrList[i]);
				}
				//得到bean的属性值
				Object attrValue = null;
				try {
					attrValue = pd.getReadMethod().invoke(bean, null);
				} catch (Exception e) {
					throw new RuntimeException("无法获取bean的属性值：" + pd.getName());
				}
				//转成字符串
				String cellValue = "";
				if(attrValue instanceof Date){
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					df.format(attrValue);
				}else{
					if(attrValue!=null){
						cellValue = attrValue.toString();
					}
				}
				Cell cell = row.createCell(i);
				cell.setCellStyle(createDataStyle(workbook));
				cell.setCellValue(cellValue);
			}
		}
		try {
			workbook.write(out);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * @param title：excel文件名称
	 * @param headers：表头
	 * @param dataset：要保存到表格的对象集合
	 * @param out：输出目的地
	 */
	public static void exportExcel2(String title, String[] headers, Collection<Object[]> dataset,OutputStream out) {

		// 声明一个工作薄
		Workbook workbook = new HSSFWorkbook();

		// 生成一个表格
		Sheet sheet = workbook.createSheet(title);

		// 设置表格默认列宽度为15
		//sheet.setDefaultColumnWidth((short) 20);
		//sheet.setColumnWidth(1, 100 * 256);
		//设置表格宽度自适应
		sheet.autoSizeColumn(1, true);
		// 产生表头
		Row row = sheet.createRow(0);

		//插入表头数据
		for (short i = 0; i < headers.length; i++) {
			Cell cell = row.createCell(i);
			//设置样式
			cell.setCellStyle(createHeaderStyle(workbook));
			HSSFRichTextString text = new HSSFRichTextString(headers[i]);
			cell.setCellValue(text);
		}

		int index = 1;
		for(Object[] bean : dataset){
			//得到一个bean，则生成表格的一行
			row = sheet.createRow(index++);
			for(int i=0;i<bean.length;i++){
				//转成字符串
				String cellValue = null;
				if(bean[i] == null){
					 cellValue = "null";
				}else{
					 cellValue = bean[i].toString();
				}

				Cell cell = row.createCell(i);
				cell.setCellStyle(createDataStyle(workbook));
				cell.setCellValue(cellValue);
			}
		}
		try {
			workbook.write(out);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	public static void   exportExcelXSSF(String title, String[] headers, Collection<Object[]> dataset,OutputStream out) throws IOException {
		//声明一个工作薄
		XSSFWorkbook wb = new XSSFWorkbook();
		//声明一个表格
		XSSFSheet sheet = wb.createSheet(title);
		//设置表格宽度自适应
		//sheet.autoSizeColumn(1, true);
		sheet.setColumnWidth( 1,100*256 );
		// 产生表头
		XSSFRow row = sheet.createRow(0);
		//插入表头数据
		for (short i = 0; i < headers.length; i++) {
			XSSFCell cell = row.createCell(i);
			//设置样式
			cell.setCellStyle(createHeaderStyle(wb));

			XSSFRichTextString text=new XSSFRichTextString( headers[i] );

			cell.setCellValue(text);
		}
		int index=1;
		for (Object[] object : dataset) {
			//得到一个bean。生成表格的一行
			row=sheet.createRow( index++ );
			String substring=null;
			for (int i=0;i<object.length;i++){
				if (object[i].toString().length()>SpreadsheetVersion.EXCEL2007.getMaxTextLength()){
					substring = object[i].toString().substring( 0, 32766 );
				}else {
					substring = object[i].toString();
				}
				//转成字符串
				//String cellvalue=null;
				if ( substring==null){
					substring="null";
				}/*else {
					if(object[i].toString().length()>SpreadsheetVersion.EXCEL2007.getMaxTextLength() ){
						System.out.println( "导出单元格字数过大" );
						wb.close();

					}else {
						cellvalue = object[i].toString();
					}
				}*/
				XSSFCell xssfCell=row.createCell( i );
				xssfCell.setCellStyle(createDataStyle(wb));
				xssfCell.setCellValue(substring);
			}
		}

		try {
			wb.write(out);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
	public void setCellValue(RichTextString str) {

		if (str.length() > SpreadsheetVersion.EXCEL2007.getMaxTextLength()) {
			throw new IllegalArgumentException( "The maximum length of cell contents (text) is 32,767 characters" );
		}
	}
	private static CellStyle createHeaderStyle(Workbook workbook){
		// 生成一个样式
		CellStyle style = workbook.createCellStyle();

		// 设置表头的样式
		style.setFillForegroundColor(HSSFColor.WHITE.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
		style.setBorderRight(HSSFCellStyle.BORDER_THIN);
		style.setBorderTop(HSSFCellStyle.BORDER_THIN);
		style.setAlignment(HSSFCellStyle.ALIGN_CENTER);

		// 生成表头的字体
		Font font = workbook.createFont();
		font.setColor(HSSFColor.VIOLET.index);
		font.setFontHeightInPoints((short) 12);
		font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
		// 把字体应用到当前的样式
		style.setFont(font);
		return style;
	}

	private static CellStyle createDataStyle(Workbook workbook){

		// 生成数据行的样式
		CellStyle style = workbook.createCellStyle();
		style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
		style.setBorderRight(HSSFCellStyle.BORDER_THIN);
		style.setBorderTop(HSSFCellStyle.BORDER_THIN);
		style.setAlignment(HSSFCellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);
		// 生成数据行的字体
//		Font font = workbook.createFont();
//		font.setBoldweight(HSSFFont.BOLDWEIGHT_NORMAL);
//		// 把字体应用到当前的样式
//		style.setFont(font);
		return style;
	}

}
