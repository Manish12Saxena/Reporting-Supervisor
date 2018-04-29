package reporting.supervisor;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

public class ConnectionProvider {

	static Logger logger = Logger.getLogger(ConnectionProvider.class);

	private ConnectionProvider() {
		super();
	}
	
	public static Connection getConnection()
	{
		BasicDataSource ds = ConnectionProviderHolder.ds;
		
		try 
		{
			return ds.getConnection();
		} 
		catch (SQLException e) {
			logger.error("Some error occurred while getting connection. " + e.getMessage());
			logger.info("Retrying to get connection");
			
			return getConnection();
		}
	}

	private static class ConnectionProviderHolder
	{
		private ConnectionProviderHolder() {}
		
		private static BasicDataSource ds = buildDataSource();

		private static BasicDataSource buildDataSource() 
		{
			String ip = PropertiesData.getValue("IP");
			String userId = PropertiesData.getValue("USERID");
			String password = PropertiesData.getValue("PASSWORD");
			String rdbms = PropertiesData.getValue("DBTYPE");
			String port = PropertiesData.getValue("PORT");
			String database = PropertiesData.getValue("DATABASE");
			
			  BasicDataSource ds = new BasicDataSource();
		      ds.setUsername(userId);
		      ds.setPassword(password);
		      if(rdbms.equalsIgnoreCase("ORACLE"))
		        {
  			        ds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
  			        ds.setUrl("jdbc:oracle:thin:@" + ip + ":" +port+ ":" + database);
		        }
		        else if(rdbms.equalsIgnoreCase("MYSQL"))
		        {
		            ds.setDriverClassName("com.mysql.jdbc.Driver");
  				ds.setUrl("jdbc:mysql://"
  					+ ip
  					+ ":"
  					+ port
  					+ "/"
  					+ database);

		        }
		      
		      	ds.setMaxTotal(Integer.parseInt(PropertiesData.getValue("MAX_ACTIVE_CONNECTION")));
		        ds.setMaxIdle(Integer.parseInt(PropertiesData.getValue("MAX_IDLE_CONNECTION")));
		        ds.setMaxWaitMillis(Integer.parseInt(PropertiesData.getValue("MAX_WAIT_TIMEOUT_IN_SEC")));
		        
			return ds;
		}
		
		
	}
}
