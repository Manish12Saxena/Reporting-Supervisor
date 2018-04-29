package reporting.supervisor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

public class PropertiesData {

	static Properties properties = new Properties();
	static Logger logger = Logger.getLogger(PropertiesData.class);

	static {
	//	DOMConfigurator.configure("resources/log4j.xml");
	 DOMConfigurator.configure("E:\\ReportingSupervisor\\src\\main\\resources\\log4j.xml");

		try {
			InputStream fis = PropertiesData.class.getClassLoader()
					.getResourceAsStream("RS_PROPERTY.properties");
			properties.load(fis);

			String configTable = PropertiesData.getValue("CONFIGURATION_TABLE");
			Connection con = ConnectionProvider.getConnection();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select * from " + configTable
					+ " ");
			while (rs.next()) {
				properties.put("REPORT_TABLE", rs.getString("REPORT_TABLE"));
				properties.put("EMP_TABLE", rs.getString("EMP_TABLE"));
				properties.put("EXCLUDE_EMP_TABLE", rs.getString("EXCLUDE_EMP_TABLE"));
				properties.put("TEMPLATE_DEFN_TABLE",
						rs.getString("TEMPLATE_DEFN_TABLE"));
				properties.put("REPORT_PROCEDURE", rs.getString("REPORT_PROCEDURE"));
				properties.put("EMAIL_ENGINE_PROCEDURE", rs.getString("EMAIL_BODY_PROCEDURE"));
				properties.put("EMP_EMAIL", rs.getString("EMP_EMAIL"));
				properties.put("EMP_CODE", rs.getString("EMP_CODE"));
				properties.put("EMP_ID", rs.getString("EMP_ID"));
				properties.put("EMP_ROLE", rs.getString("EMP_ROLE"));
			}
		} catch (Exception e) {

			logger.error(e.getMessage(), e);

		}

		logger.info("Logging has been implemented");
	}

	private PropertiesData() {
		super();
	}

	public static String getValue(String key) {
		return properties.getProperty(key);
	}
}
