package reporting.supervisor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import model.Constants;

public class MakeReports {

	static Logger logger = Logger.getLogger(MakeReports.class);

	private MakeReports() {
		super();
	}

	public static File createCsv(ResultSet resultSet, String filePathInput, String fileName) {

		String filePath = "";
		if (!filePathInput.equals("")) {
			filePath = filePathInput + File.separator;
		}
		File file = new File(filePath + fileName + ".csv");

		try (FileWriter fw = new FileWriter(file)) {
			ResultSetMetaData metaData = resultSet.getMetaData();
			CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");
			CSVPrinter printer = new CSVPrinter(fw, format);

			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				printer.print(metaData.getColumnName(i));
			}
			printer.println();
			printer.printRecords(resultSet);
			printer.close();

		} catch (Exception ex) {
			logger.error("Error in creating csv file.  \n");
			logger.error(ex.getMessage(), ex);
		}
		return file;
	}

	public static File createExcel(ResultSet resultSet, String filePathInput, String fileName,
			Map<String, Object> templateMap, Map<String, String> procedureParam, Connection con)
			throws SQLException, IOException {

		HashMap sheetNameMap = new HashMap<>();
		File file = null;
		int numOfRows = (int) templateMap.get("rowCount");
		String useTemplate = (String) templateMap.get("USE_TEMPLATE");
		
		String filename = (!filePathInput.isEmpty() ? filePathInput + File.separator : "") + fileName + ".xlsx";
		
		String sheetName = "Data";

		SXSSFWorkbook workbook = null;
		Sheet worksheet = null;

		int isReadTemplate = 0;
		if (useTemplate.equals("1")) {
			isReadTemplate = 1;
			String templateFile = (String) templateMap.get("PATH");

			logger.info("Using template: " + templateFile);
			InputStream fileInputStream = new FileInputStream(templateFile);
			XSSFWorkbook book = new XSSFWorkbook(fileInputStream);
			fileInputStream.close();
			workbook = new SXSSFWorkbook(book);
			sheetNameMap = (HashMap) templateMap.get("SHEETNAME");

			if (numOfRows == 1) {
				sheetName = (String) sheetNameMap.get(1);
				logger.info("Sheet name: " + sheetName);
				worksheet = workbook.getSheet(sheetName);
			}

			if (worksheet == null && numOfRows == 1) {
				logger.info("Sheet name got from template reference table is not available in the given Template.");

			}

		} else {
			workbook = new SXSSFWorkbook();
			worksheet = workbook.createSheet(sheetName);

			logger.info("NT " + worksheet);
		}
		file = new File(filename);

		if(!file.exists())
		{
			if(!file.getParentFile().exists())
			{
				file.getParentFile().mkdirs();
			}
			
			file.createNewFile();
		}
		try (FileOutputStream out = new FileOutputStream(file);) {
			int rowcount = isReadTemplate;
			int coloumncount = 0;

			if (useTemplate.equals("0")) {
				writeIntoSheet(worksheet, isReadTemplate, rowcount, coloumncount, resultSet);
			} else {
				HashMap procedureNameMap = (HashMap) templateMap.get(Constants.PROCEDURE_LIST);
				String procedureName = "";
				for (int i = 1; i <= numOfRows; i++) {
					isReadTemplate = 1;
					rowcount = isReadTemplate;
					coloumncount = 0;
					sheetName = (String) sheetNameMap.get(i);
					logger.info("Sheet name: " + sheetName);
					worksheet = workbook.getSheet(sheetName);
					procedureName = (String) procedureNameMap.get(i);

					String dbType = PropertiesData.getValue("DBTYPE");

					ResultSet resultset = dbType.equalsIgnoreCase("MYSQL")
							? Call.mysqlProcedure(procedureName, procedureParam, con)
							: Call.oracleProcedure(procedureName, procedureParam, con);

					writeIntoSheet(worksheet, isReadTemplate, rowcount, coloumncount, resultset);
				}

			}

			workbook.write(out);

		} catch (Exception e) {
			logger.error(e);
		} finally {
			workbook.dispose();
		}

		return file;
	}

	public static void writeIntoSheet(Sheet worksheet, int isReadTemplate, int rowcount, int coloumncount,
			ResultSet resultSet) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCnt = coloumncount;
		int rowCount = rowcount;

		if (worksheet == null)
			return;

		if (isReadTemplate == 0) {
			Row row = worksheet.createRow(rowCount++);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				Cell cell = row.createCell(columnCnt++);
				String coloumnname = metaData.getColumnName(i);
				cell.setCellValue(coloumnname);
				System.out.println(coloumnname);
			}
		}

		while (resultSet.next()) {
			columnCnt = 0;
			Row row1 = worksheet.createRow(rowCount++);
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				Cell cell = row1.createCell(columnCnt++);

				int columnType = resultSet.getMetaData().getColumnType(i);
				
				switch(columnType)
				{
				case Types.VARCHAR:
					cell.setCellValue(resultSet.getString(i));
					break;
				case Types.FLOAT:
					cell.setCellValue(resultSet.getFloat(i));
					break;
				case Types.DOUBLE:
					cell.setCellValue(resultSet.getDouble(i));
					break;
				case Types.INTEGER:
				case Types.NUMERIC:
					cell.setCellValue(resultSet.getInt(i));
					break;
				case Types.BIGINT:
					cell.setCellValue(resultSet.getInt(i));
					break;
					default:
						cell.setCellValue(resultSet.getString(i));	
				break;
				
				}
			}
		}
	}

	public static void makePasswordProtect(String password, File file) {
		POIFSFileSystem fs = new POIFSFileSystem();
		try (OPCPackage opc = OPCPackage.open(file, PackageAccess.READ_WRITE);) {
			EncryptionInfo info = new EncryptionInfo(fs, EncryptionMode.agile);
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(password);
			OutputStream os = enc.getDataStream(fs);
			opc.save(os);
			opc.close();
			FileOutputStream fos = new FileOutputStream(file);
			fs.writeFilesystem(fos);
			fos.close();
		} catch (InvalidFormatException | IOException | GeneralSecurityException e) {
			logger.error("Error in making excel file password protected.  \n");
			logger.error(e.getMessage(), e);
		}
	}

	public static File zipFile(File inputFile) {
		File f = null;
		byte[] buf = new byte[1024];
		String prevExt = inputFile.getName().substring(inputFile.getName().indexOf('.'));
		String target = inputFile.getAbsolutePath().replace(prevExt, ".zip");
		try (

				FileOutputStream fos = new FileOutputStream(target);
				ZipOutputStream out = new ZipOutputStream(fos);

				FileInputStream in = new FileInputStream(inputFile);) {
			out.putNextEntry(new ZipEntry(inputFile.getName()));

			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}

			out.closeEntry();

			in.close();

			out.close();
			if (!inputFile.delete()) {
				logger.error("File not deleted");
			}
			f = new File(inputFile.getAbsolutePath().replace(prevExt, ".zip"));

		} catch (IOException e) {
			logger.error("Error in making zip file.  \n");
			logger.error(e.getMessage(), e);
		}
		return f;
	}

}
