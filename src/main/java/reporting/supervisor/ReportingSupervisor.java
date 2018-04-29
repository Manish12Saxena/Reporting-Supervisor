package reporting.supervisor;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import model.Constants;
import model.EmpModel;
import model.ReportModel;
import oracle.jdbc.OracleTypes;

public class ReportingSupervisor 
{
	static String orgId = PropertiesData.getValue("ORGID");
	static String reportTable = PropertiesData.getValue("REPORT_TABLE");
	static String empTable = PropertiesData.getValue("EMP_TABLE");
	static String excludeEmpTable = PropertiesData.getValue("EXCLUDE_EMP_TABLE");
	static String templateDefnTable = PropertiesData.getValue("TEMPLATE_DEFN_TABLE");


	static Logger logger = Logger.getLogger(ReportingSupervisor.class);
	
	   public static String getSaltString(int digit) {
			String saltChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
			StringBuilder salt = new StringBuilder();
			Random rnd = new Random();
			while (salt.length() < digit) { // length of the random string.
			    int index = (int) (rnd.nextFloat() * saltChars.length());
			    salt.append(saltChars.charAt(index));
			}
			return salt.toString();
	   }

	public boolean checkSendTimeAndLastAttempt(Calendar currTimeCal, String sendTime, String lAttemptTime,
			String attemptStatus) {

		logger.info("Send time: " + sendTime);
		
		boolean result = false;

		Date oneHourBack = new Date(System.currentTimeMillis() - 3600 * 1000);
		Calendar oneHourBackCal = Calendar.getInstance(TimeZone.getDefault());
		oneHourBackCal.setTime(oneHourBack);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.UK);
		int lastAttemptDate = 0;
		Calendar lastAttemptTime = null;
		try {
			sdf.parse(lAttemptTime);
			lastAttemptTime = sdf.getCalendar();
			lastAttemptDate = lastAttemptTime.get(Calendar.DATE);
		} catch (Exception e) {

			logger.error("Error in parsing last attempt time using SDF. " + e);
		}

		int currDate = currTimeCal.get(Calendar.DATE);

		if (lastAttemptTime == null || (lastAttemptTime.before(currTimeCal) && lastAttemptDate < currDate)
				|| (null != attemptStatus && !attemptStatus.equalsIgnoreCase(Constants.SUCCESS_CHK))) {
			result = true;
		}

		return result;
	}

	public boolean checkIfToSendNow(String dow, String dom, String sendTime, String lastAttemptTime,
			String attemptStatus, ReportModel inputReport) throws ParseException {
		boolean sendNow = false;

		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		int curDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		int curDayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
		String[] daysOfMonth = null;
		String[] daysOfWeek = null;

		if (dom != null && dom.contains(",")) 
		{
			daysOfMonth = dom.trim().split(",");
			sendNow = Arrays.asList(daysOfMonth).contains(String.valueOf(curDayOfMonth)) || (dom.trim().equals(String.valueOf(curDayOfMonth))) ? true : false;
		} 
		else 
		{
			sendNow = true;
		}

		if (dow != null && dow.trim().contains(",") && sendNow) 
		{
			daysOfWeek = dow.split(",");
			sendNow = Arrays.asList(daysOfWeek).contains(String.valueOf(curDayOfWeek)) || (dow.trim().equals(String.valueOf(curDayOfWeek))) ? true : false;
		}

		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		Date d = df.parse(sendTime);
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		Calendar currentTime = Calendar.getInstance();
		currentTime.setTime(df.parse(df.format(new Date())));

		if (cal.getTimeInMillis() <= currentTime.getTimeInMillis() || attemptStatus.equals(Constants.FAILURE_CHK)) 
		{
			sendNow = inputReport.getDailyFrequency() != null ? true : checkSendTimeAndLastAttempt(calendar, sendTime, lastAttemptTime, attemptStatus);
		}

		return sendNow;
	}

	public ResultSet getAllEmpToSendReport(String role, List<String> excludedEmpList, Connection con) throws SQLException {
		String empRole = PropertiesData.getValue("EMP_ROLE");
		String empId = PropertiesData.getValue(Constants.EMP_ID);
		String empEmail = PropertiesData.getValue(Constants.EMP_EMAIL);
		String empCode = PropertiesData.getValue("EMP_CODE");

		String query = "select " + empRole + ", " + empId + ", " + empEmail + ", " + empCode + " from " + empTable + " "
				+ Constants.WHERE  + " "+ empRole + "='" + role + "'";

		StringBuilder builderQuery = new StringBuilder(query);
		
		if (!excludedEmpList.isEmpty()) 
		{
			builderQuery.append(" and ").append(empId).append(" not in (");
			for(String excludeEmpId : excludedEmpList)
			{
				builderQuery.append("'").append(excludeEmpId).append("'").append(",");
			}
			
			query = builderQuery.substring(0, builderQuery.length() -1);
			query += ")";
		}
		
		ResultSet set = null;
		Statement statement = null;
		try 
		{
			statement = con.createStatement();
			set = statement.executeQuery(query);
		} catch (Exception e) {

			logger.error(Constants.ERROR1);
			logger.error(e.getMessage(), e);
		}

		return set;
	}

	public List<String> excludeEmp(String reportId, Connection con) throws SQLException 
	{
		List<String> allExcludedEmps = new ArrayList<>();
		
		String query = "SELECT empids from " + excludeEmpTable + " where report_id=" + reportId;
		try (Statement statement = con.createStatement(); ResultSet exResultSet = statement.executeQuery(query);) 
		{
			while (exResultSet.next()) 
			{
				String excludedEmps = exResultSet.getString("empids");
				if (excludedEmps.contains(",")) {
					String[] ids = excludedEmps.trim().split(",");
					for (String excludeEmpId : ids) 
					{
						allExcludedEmps.add(excludeEmpId);
					}
				} 
				else 
				{
					allExcludedEmps.add(excludedEmps);
				}
			}
		}
		
		return allExcludedEmps;
	}

	public String createReport(EmpModel empModel, ReportModel reportModel, Connection con) throws SQLException, IOException  {
		File report = null;
		String reportAbsolutePath = "";
		ResultSet reportDataRS = null;
		HashMap<String, String> procedureParam = new HashMap<>();

		try {
			String empId = empModel.getEmpId();
			String empRole = empModel.getEmpRole();
			String reportHeader = reportModel.getReportHeader();
			String procedure = reportModel.getReportSP();
			String currDate = getCurrentDateTime("onlyDate");
			String reportFilename = reportModel.getSendSameDataToAll().trim().equalsIgnoreCase("Y")
					? reportModel.getReportFilename()
					: empId + "_" + reportModel.getReportFilename()+"_"+getSaltString(6);
			String useTemplate = reportModel.getUseTemplate();
			String outputFilePath = reportModel.getOutputFilePath();

			Map<String, Object> templateMap = getTemplateData(con, useTemplate, reportModel);

			procedureParam.put("1", orgId);
			procedureParam.put("2", empId);
			procedureParam.put("3", empRole);
			procedureParam.put("4", reportHeader);
			procedureParam.put("5", currDate);

			if (!useTemplate.equalsIgnoreCase("1")) {
				String dbType = PropertiesData.getValue(Constants.DBTYPE);
				
				reportDataRS = dbType.equalsIgnoreCase(Constants.MYSQL) ?  Call.mysqlProcedure(procedure, procedureParam, con) : Call.oracleProcedure(procedure, procedureParam, con);  
			}

			if (reportModel.getReportBody() != null && (!reportModel.getReportBody().equals(""))) {
				report = MakeReports.createExcel(reportDataRS, outputFilePath, reportFilename, templateMap,
						procedureParam, con);
			} else if (reportDataRS != null && reportDataRS.next()) {
				reportModel.setReportBody(reportDataRS.getString("email_body"));
			}

			if (reportModel.getEditPassword() != null && (!reportModel.getEditPassword().equals(""))) {
				MakeReports.makePasswordProtect(reportModel.getEditPassword(), report);
			}

			if (reportModel.getSendAsZip() != null && reportModel.getSendAsZip().equals("1")) {
				MakeReports.zipFile(report);
			}
			if (report != null) {
				reportAbsolutePath = report.getAbsolutePath();
			}

		} finally {
			try {
				if (reportDataRS != null) {
					reportDataRS.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.ERROR2+ e);
			}
		}

		return reportAbsolutePath;
	}

/*	public String createReport1(ReportModel reportModel, Connection con) {
		File report = null;
		String reportAbsolutePath = "";
		ResultSet reportDataRS = null;
		HashMap<String, String> procedureParam = new HashMap<>();

		try {

			String reportHeader = reportModel.getReportHeader();
			String procedure = reportModel.getReportSP();
			String currDate = getCurrentDateTime("onlyDate");
			String reportFilename = reportModel.getReportFilename();
			String useTemplate = reportModel.getUseTemplate();
			String outputFilePath = reportModel.getOutputFilePath();

			Map<String, Object> templateMap = getTemplateData(con, useTemplate, reportModel);

			procedureParam.put("1", orgId);
			procedureParam.put("2", "");
			procedureParam.put("3", "");
			procedureParam.put("4", reportHeader);
			procedureParam.put("5", currDate);

			String dbType = PropertiesData.getValue(Constants.DBTYPE);
			if (dbType.equalsIgnoreCase(Constants.MYSQL)) {
				reportDataRS = Call.mysqlProcedure(procedure, procedureParam, con);
			} else if (dbType.equalsIgnoreCase(Constants.ORACLE)) {
				reportDataRS = Call.oracleProcedure(procedure, procedureParam, con);
			}

			if (reportModel.getReportBody() != null && (!reportModel.getReportBody().equals(""))) {
				report = MakeReports.createExcel(reportDataRS, outputFilePath, reportFilename, templateMap,
						procedureParam, con);
			} else if (reportDataRS != null && reportDataRS.next()) {
				reportModel.setReportBody(reportDataRS.getString("email_body"));
			}

			if (reportModel.getEditPassword() != null && (!reportModel.getEditPassword().equals(""))) {
				MakeReports.makePasswordProtect(reportModel.getEditPassword(), report);
			}

			if (reportModel.getSendAsZip() != null && reportModel.getSendAsZip().equals("1")) {
				MakeReports.zipFile(report);
			}
			if (report != null) {
				reportAbsolutePath = report.getAbsolutePath();
			}

		} catch (Exception e) {

			logger.error(Constants.ERROR3);
			logger.error(e.getMessage(), e);
		} finally {
			try {
				if (reportDataRS != null) {
					reportDataRS.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.ERROR2+ e);
			}
		}

		return reportAbsolutePath;
	}
*/
	public Map<String, Object> getTemplateData(Connection con, String useTemplate, ReportModel reportModel) {
		int rowCount = 0;
		HashMap<Integer, String> sheetNameMap = new HashMap<>();
		HashMap<Integer, String> procedureMap = new HashMap<>();
		HashMap<String, Object> templateMap = new HashMap<>();

		if (useTemplate.equals("1")) {
			Statement statement=null;
			try {
					statement = con.createStatement();
					ResultSet templateResultSet = statement.executeQuery(Constants.SELECT_STAR_FROM + templateDefnTable
							+ " WHERE report_id=" + reportModel.getReportId());
		
			
				templateMap.put("USE_TEMPLATE", "1");
				templateMap.put("PATH", reportModel.getFullTemplateFileName());
				while (templateResultSet.next()) {
					System.out.println(templateResultSet.getString("EXCEL_SHEET_NAME"));
					rowCount++;
					templateMap.put("rowCount", rowCount);
					sheetNameMap.put(rowCount, templateResultSet.getString("excel_sheet_name") == null ? "Data"
							: templateResultSet.getString("excel_sheet_name"));
					procedureMap.put(rowCount, templateResultSet.getString("refresh_SP"));

				}

				templateMap.put(Constants.SHEET_NAME, sheetNameMap);
				templateMap.put(Constants.PROCEDURE_LIST, procedureMap);

			} catch (Exception e) {
				logger.error(e);
			}
		} else {
			templateMap.put("USE_TEMPLATE", "0");
			templateMap.put("PATH", "");
			templateMap.put("rowCount", 0);
			templateMap.put(Constants.SHEET_NAME, "");
		}
		return templateMap;
	}

	public ReportModel getReportObjectWithData(ResultSet resultSet) {
		ReportModel model = new ReportModel();

		try {

			model.setReportId(resultSet.getString("report_id"));
			model.setReportHeader(resultSet.getString("report_header"));
			model.setReportSP(resultSet.getString("report_SP"));
			model.setDaysOfWeek(resultSet.getString("days_of_week"));
			model.setDaysOfMonth(resultSet.getString("days_of_month"));
			model.setReportStatus(resultSet.getString("report_status"));
			model.setReportToRoles(resultSet.getString("report_to_roles"));
			model.setReportToEmpid(resultSet.getString("report_to_empid"));
			model.setCcTo(resultSet.getString("cc_to"));
			model.setSendTime(resultSet.getString("send_time"));
			model.setOutgoingTime(resultSet.getString("outgoing_time"));
			model.setReportDeliveryMode(resultSet.getString("report_deliverymode"));
			model.setReportBody(resultSet.getString("report_body"));
			model.setLastAttemptTime(resultSet.getString("last_attempt_time"));
			model.setLastAttemptBy(resultSet.getString("last_attempt_by"));
			model.setAttemptRemarks(resultSet.getString("attempt_remarks"));
			model.setAttemptStatus(resultSet.getString("attempt_status"));
			model.setSendLateReport(resultSet.getString("sendLateReport"));
			model.setReportFilename(resultSet.getString("report_filename"));
			model.setUseTemplate(resultSet.getString("use_template"));
			model.setFullTemplateFileName(resultSet.getString("full_template_file_name"));
			model.setEditPassword(resultSet.getString("edit_password"));
			model.setSendAsZip(resultSet.getString("send_as_zip"));
			model.setHasData(resultSet.getString("hasdata"));
			model.setFtpFileStorePath(resultSet.getString("Ftp_File_Store_Path"));
			model.setShareFileVia(resultSet.getString("share_file_via"));
			model.setHttpPath(resultSet.getString("http_path"));
			model.setOutputFilePath(resultSet.getString("output_file_path"));
			model.setDailyFrequency(resultSet.getString("daily_frequency"));
			model.setSendSameDataToAll(resultSet.getString("send_same_data_to_all"));
			model.setIsMailToBeSent(resultSet.getString("IS_MAIL_TO_BE_SENT"));
			model.setEmailProfileId(resultSet.getString("email_profile_id"));
			model.setMailBodyContent(resultSet.getString("EMAIL_BODY_CONTENT_SP"));
			model.setMailBodyTemplate(resultSet.getString("EMAIL_BODY_TEMPLATE"));
			model.setMailBodyType(resultSet.getString("EMAIL_BODY_TYPE"));

		} catch (Exception e) {

			logger.error("Error in setting data in ReportModel - getReportObjectWithData method.  \n");
			logger.error(e.getMessage(), e);
		}

		return model;
	}

	public EmpModel getEmpObjectWithData(ResultSet resultSet) throws SQLException
	{
		EmpModel empModel = new EmpModel();

		empModel.setEmpId(resultSet.getString(PropertiesData.getValue("EMP_ID")));
		empModel.setEmpEmailId(resultSet.getString(PropertiesData.getValue("EMP_EMAIL")));
		empModel.setEmpRole(resultSet.getString(PropertiesData.getValue("EMP_ROLE")));
		empModel.setEmpCode(resultSet.getString(PropertiesData.getValue("EMP_CODE")));

		return empModel;
	}

	public String getEmailProfileId(Connection con) throws SQLException {
		String profileId = "";
		try (Statement statement = con.createStatement();
				ResultSet resultSet = statement
						.executeQuery("Select profile_id from org_email_profiles where org_id='" + orgId + "'");) 
		{

			resultSet.next();
			profileId = resultSet.getString(1);
		}

		return profileId;
	}

	public boolean checkOnFreqBase(ReportModel reportModel) {
		boolean result = false;
		String lastAttempt = reportModel.getLastAttemptTime();
		String lastAttemptStatus = reportModel.getAttemptStatus();

		if (lastAttempt == null || lastAttempt.trim().isEmpty() || null == lastAttemptStatus
				|| lastAttemptStatus.equalsIgnoreCase(Constants.FAILURE_CHK)) {
			result = true;

		}
		return result;
	}

	public boolean sendMail(ReportModel reportModel, File report, String recipientEmail, Connection con)
			throws SQLException {
		boolean sent = false;

		String dbType = PropertiesData.getValue(Constants.DBTYPE);
		HashMap<String, String> emailParameter = new HashMap<>();

		String emailSubject = reportModel.getReportHeader();
		String emailBody = "";
		String cc = reportModel.getCcTo();
		String bcc = "";
		String httpPath = reportModel.getHttpPath();
		String fileName = "";
		if (report != null) {
			fileName = report.getName();
		}
		String outputFilePath = reportModel.getOutputFilePath();
		String attachment = outputFilePath + "/" + fileName;

		String currDateTime = getCurrentDateTime("");

	if(reportModel.getMailBodyType()!=null )
	{
		String  body1=reportModel.getMailBodyTemplate();
		String procedure=reportModel.getMailBodyContent();
		ResultSet rs1=Call.oracleProcedure(procedure, emailParameter, con);
		ArrayList<String> ar=new ArrayList<String>();
		ResultSetMetaData rsmd=rs1.getMetaData();
		int noOfColumns=rsmd.getColumnCount();
		   while(rs1.next())
		   { 
			   
			  for(int i=1;i<=noOfColumns;i++)
			ar.add(rs1.getString(i));
			
		   }
		   String s2=null;
		   for(int i=1;i<=ar.size();i++)
			{
			 s2=body1.replace("&"+i, ar.get(i-1));
			body1=s2;
			
			}
			emailBody=body1;
	}
	else
	{
	//	if (reportModel.getReportBody() != null && (!reportModel.getReportBody().equals(""))) {
			emailBody = reportModel.getReportBody();
	}	

		if (reportModel.getShareFileVia().equalsIgnoreCase("url")) {
			String[] httpPathArr = httpPath.split("~");
			String url = outputFilePath.replace(httpPathArr[0], httpPathArr[1]) + fileName;
			emailBody = emailBody.replace("<~URL~>", url);
			attachment = "";
		}

		emailParameter.put("1", orgId);
		emailParameter.put("2", reportModel.getEmailProfileId());
		emailParameter.put("3", emailSubject);
		emailParameter.put("4", emailBody);
		emailParameter.put("5", recipientEmail);
		emailParameter.put("6", cc);
		emailParameter.put("7", bcc);
		emailParameter.put("8", emailSubject);
		emailParameter.put("9", currDateTime);
		emailParameter.put("10", attachment);
		emailParameter.put("11", "");

		ResultSet rs = null;
		try 
		{
			if (dbType.equalsIgnoreCase(Constants.MYSQL)) {
				rs = Call.mysqlProcedure(PropertiesData.getValue("EMAIL_ENGINE_PROCEDURE"), emailParameter, con);
			} else if (dbType.equalsIgnoreCase(Constants.ORACLE)) {
				rs = Call.oracleProcedure(PropertiesData.getValue("EMAIL_ENGINE_PROCEDURE"), emailParameter, con);

			}
		} catch (Exception e) {
			sent = false;
			logger.error("Error in DB Connection." + e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.ERROR2+ e);
			}
		}

		return sent;
	}

	public String getCurrentDateTime(String withTime) {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateFormat = null;
		if (withTime.equalsIgnoreCase("Time")) {
			dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		} else {
			dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		}
		return dateFormat.format(calendar.getTime());
	}

	public String updateSentTime(String sentTime, String dailyFreq) throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		Date d = df.parse(sentTime);
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.add(Calendar.MINUTE, Integer.parseInt(dailyFreq));

		Calendar currentTime = Calendar.getInstance();
		currentTime.setTime(df.parse(df.format(new Date())));

		while (cal.getTimeInMillis() < currentTime.getTimeInMillis()) {
			cal.add(Calendar.MINUTE, Integer.parseInt(dailyFreq));
		}

		return df.format(cal.getTime());

	}



	public void updateReportTable(String reportId, String attemptStatus, Connection con, String dailyFreq,
			String sentTime, String lastAttemptStatus) throws ParseException, SQLException {

		String query = "";

		if (attemptStatus.equals("") && !dailyFreq.isEmpty()
				&& !lastAttemptStatus.equalsIgnoreCase(Constants.FAILURE_CHK)) {

			query = Constants.UPDATE_TABLE + reportTable + " SET last_attempt_time='" + getCurrentDateTime("Time")
					+ "', SEND_TIME = '" + updateSentTime(sentTime, dailyFreq)
					+ "' ,last_attempt_by='Reporting_Supervisor', attempt_status='' where report_id=" + reportId;
		} else if (attemptStatus.equals("") && dailyFreq.isEmpty()) {
			query = Constants.UPDATE_TABLE + reportTable + " SET last_attempt_time='" + getCurrentDateTime("Time")
					+ "', last_attempt_by='Reporting_Supervisor', attempt_status='' where report_id=" + reportId;
		} else if (attemptStatus.equalsIgnoreCase(Constants.SUCCESS_CHK)) {
			query = Constants.UPDATE_TABLE + reportTable + " SET attempt_status='Success'";
		} else {
			query = Constants.UPDATE_TABLE + reportTable + " SET attempt_status='Failure'";
		}

		logger.info("update query: " + query);
		try (Statement statement = con.createStatement();) {
			statement.execute(query);
		}
	}

	public String getEmpEmail(String empId, Connection con) throws SQLException {
		String email = "";
		String query = "SELECT " + PropertiesData.getValue("EMP_EMAIL") + " FROM " + empTable + " " + Constants.WHERE
				+ " "+ PropertiesData.getValue("EMP_ID") + "=?";
		ResultSet resultSet = null;
		try (PreparedStatement statement = con.prepareStatement(query);) {
			statement.setString(1, empId);
			resultSet = statement.executeQuery();
			resultSet.next();
			email = resultSet.getString(1);
		} catch (Exception e) {

			logger.error("Error in getting Employee email.  \n");
			logger.error(e.getMessage(), e);
		} finally {
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}
		}

		return email;
	}

	public void worker(boolean send, int rId, boolean sendEmail) throws SQLException {

		Connection connection = ConnectionProvider.getConnection();
		boolean ifSend = false;
		boolean checkTimeCondition = false;
		ResultSet resultSet = null;
		String procName = PropertiesData.getValue("REPORT_PROCEDURE");
		
		String query = PropertiesData.getValue(Constants.DBTYPE).equalsIgnoreCase("Oracle") ? "{call "  + procName + "(?,?)}" : "{call "  + procName + "(?)}";

		try (CallableStatement statement = connection.prepareCall(query);) {

			if (send) 
			{
				logger.info("Direct Report Generation Mode.");
				ifSend = true;
				statement.setString(1, String.valueOf(rId));

			} else {
				logger.info("Indirect Report Generation Mode.");
				checkTimeCondition = true;
				statement.setString(1, null);
			}

			if (PropertiesData.getValue(Constants.DBTYPE).equalsIgnoreCase("Oracle")) {
				statement.registerOutParameter(2, OracleTypes.CURSOR);
				statement.execute();
				
				resultSet = (ResultSet) statement.getObject(2);

			} else if (PropertiesData.getValue(Constants.DBTYPE).equalsIgnoreCase("mysql")) {
				resultSet = statement.executeQuery();
			}

		
				
			while (resultSet.next()) {
				final boolean sendMail = sendEmail;
				final ReportModel currReport = getReportObjectWithData(resultSet);
				final boolean ifSendFinal = ifSend;
				final boolean checkTimeConditionFinal = checkTimeCondition;

		     	executeRunner(currReport, sendMail, ifSendFinal, checkTimeConditionFinal);	
				
				}
			
			
		} catch (Exception e) {
			logger.error("Error in worker method.  \n");
			logger.error(e.getMessage(), e);
		}
	finally {
			try {
				connection.close();
			} catch (Exception e) {
				logger.error("Error in closing connection in worker method.  \n");
				logger.error(e.getMessage(), e);
			}   
			if (null != resultSet && !resultSet.isClosed()) {
				resultSet.close();
			}
	}
		}

	

	private void executeRunner(final ReportModel currReport, final boolean sendMail, final boolean ifSendFinal, final boolean checkTimeConditionFinal) {
		Thread t = new Thread(new Runnable()
		{
			public void run() {
				try (Connection con = ConnectionProvider.getConnection()) {
					runner(currReport, con, sendMail, ifSendFinal, checkTimeConditionFinal);
				} catch (Exception e) {
					logger.error("Error in thread  \n");
					logger.error(e.getMessage(), e);
				}
			}

		});
		t.start();		
	}

	public void runner(ReportModel currReport, Connection connection, boolean sendEmail, boolean ifSend,
			boolean checkTimeCondition) {

		try {
			boolean ifError = false;
			String dailyFreq = currReport.getDailyFrequency() == null ? "" : currReport.getDailyFrequency();
			String sendTime = currReport.getSendTime();
			String lastAttemptStatus = currReport.getAttemptStatus();

			if (checkTimeCondition) {
				String daysOfWeek = currReport.getDaysOfWeek();
				String daysOfMonth = currReport.getDaysOfMonth();
				String lastAttemptTime = currReport.getLastAttemptTime();

				try {
					ifSend = checkIfToSendNow(daysOfWeek, daysOfMonth, sendTime, lastAttemptTime, lastAttemptStatus,
							currReport);
				} catch (Exception e) {
					ifError = true;
					logger.error("Error in getAllEmpToSendReport method.  \n");
					logger.error(e.getMessage(), e);
				}
			}

			if(!ifSend) {return ;}
			
				logger.info("Send Report-> " + currReport.getReportFilename() + "- ID: " + currReport.getReportId());
				try {
					updateReportTable(currReport.getReportId(), "", connection, dailyFreq, sendTime, lastAttemptStatus);
				} catch (Exception e) {
					ifError = true;
					logger.error("Error in updating Report Table.  \n");
					logger.error(e.getMessage(), e);
				}
				String reportToRoles = currReport.getReportToRoles();
				String reportId = currReport.getReportId();
				String[] targetRoles = null != reportToRoles ? reportToRoles.trim().split(",") : new String[] {};
				List<String> excludedEmpList = new ArrayList<>();
				try {
					excludedEmpList = excludeEmp(reportId, connection);
				} catch (Exception e) {
					ifError = true;
					logger.error("Error in exclude emp method.");
					logger.error(e.getMessage(), e);
				}
				String recEmpIds = currReport.getReportToEmpid();
				String isMailToBeSent = currReport.getIsMailToBeSent();

				List<String> recEmpIdsAfterExcluding = new ArrayList<>();
				
				if (targetRoles.length == 0 && null != recEmpIds && !recEmpIds.trim().isEmpty()) 
				{
					String[] empIdsList = recEmpIds.trim().split(",");

					for(String empId : empIdsList)
					{
						if(!excludedEmpList.contains(empId))
						{
							recEmpIdsAfterExcluding.add(empId);
						}
					}
					
					String[] empIds = recEmpIdsAfterExcluding.toArray(new String[recEmpIdsAfterExcluding.size()]);
					
					for (String currEmpId : empIds) {
						File report = null;
						EmpModel currEmp = new EmpModel();
						currEmp.setEmpId(currEmpId);
						String reportPath = "";
						try {
							if(currReport.getOutputFilePath()!=null)
							{
							reportPath = createReport(currEmp, currReport, connection);
							}
							else 
							{
								
							}
						} catch (Exception e) {
							ifError = true;
							logger.error(Constants.ERROR3);
							logger.error(e.getMessage(), e);
						}
						if (!reportPath.equals("")) {
							report = new File(reportPath);
						}
						if (isMailToBeSent.equalsIgnoreCase("Y")) 
						{
							String recEmailId = getEmpEmail(currEmpId, connection);
							boolean isSent = sendMail(currReport, report, recEmailId, connection);
							if (!isSent) {
								ifError = isSent;
							}
						}
					}

				} 
				else if (targetRoles.length > 0) 
				{
					for (String role : targetRoles) {
						File report = null;
						ResultSet resultSetAllEmp = null;
						try {
							resultSetAllEmp = getAllEmpToSendReport(role, excludedEmpList, connection);
							while (resultSetAllEmp.next()) {

								EmpModel currEmp = null;
								String reportPath = "";
								try 
								{
									currEmp = getEmpObjectWithData(resultSetAllEmp);
									reportPath = createReport(currEmp, currReport, connection);
								} 
								catch (Exception e) {
									ifError = true;
									logger.error("Error in getEmpObjectWithData method.  \n");
					
									logger.error(Constants.ERROR3);
									logger.error(e.getMessage(), e);
								}

								
								
								if (!reportPath.equals("")) {
									report = new File(reportPath);
								}
								if (sendEmail && (!ifError) && currReport.getReportDeliveryMode().equalsIgnoreCase("MAIL") && currEmp.getEmpEmailId() != null) {
									StringBuilder recipientEmails = new StringBuilder(currEmp.getEmpEmailId());
									if (null != recEmpIds && recEmpIds.trim().length() > 0) {
										recipientEmails = recipientEmails;
						//recipientEmails = recipientEmails.append(",").append(recEmpIds);
									}
									if (isMailToBeSent.equalsIgnoreCase("Y")) {
										sendMail(currReport, report, recipientEmails.toString(), connection);
									}
								}

							}
						} catch (Exception e) {
							ifError = true;
							logger.error("Error in getAllEmpToSendReport method.  \n");
							logger.error(e.getMessage(), e);
						} finally {
							if (resultSetAllEmp != null) {
								resultSetAllEmp.close();
							}
						}
					}

				}
				
				String status = !ifError ? Constants.SUCCESS_CHK : Constants.FAILURE_CHK;
				updateReportTable(currReport.getReportId(), status, connection, dailyFreq, sendTime, "");

		} catch (Exception ex) {
			logger.error("Error in worker method.  \n");
			logger.error(ex.getMessage(), ex);
		}

	}

	public static void main(String[] args) {

		logger.info("Starting Reporting Supervisor");
		ReportingSupervisor reportingSupervisor = new ReportingSupervisor();

		try 
		{
			reportingSupervisor.worker(true, 15,true);

		} catch (Exception e) {
			logger.error("Some error occured while generating report or sending email" + e);
		}
		logger.info("Reporting Supervisor Exiting..!");

	}

}
