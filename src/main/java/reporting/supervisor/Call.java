package reporting.supervisor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;

public class Call {

	static Logger logger = Logger.getLogger(Call.class);

	private Call() {
		super();
		
	}

	public static ResultSet mysqlProcedure(String procedureName, Map<String,String> parameter,Connection con) throws SQLException  {
		ResultSet resultSet= null;
		int nosOfParam=parameter.size();
		StringBuilder queryBuilder= new StringBuilder("{CALL "+procedureName+"(");
		for(int i=1;i<=nosOfParam;i++) {
			queryBuilder.append("'"+ parameter.get(String.valueOf(i))+"',");
		}
		String query = queryBuilder.toString();
		query= query.substring(0, query.length()-1);
		query=query+")}";
		
		logger.info("mysql call: "+query);
		try
		{
			CallableStatement callableStatement= con.prepareCall(query);
					
			callableStatement.execute();
			resultSet= callableStatement.getResultSet();
		} 
		catch (SQLException e) 
		{
			throw new SQLException(e);
		}
		
		return resultSet;
	}

	public static ResultSet oracleProcedure(String procedureName,
			Map<String, String> parameter, Connection con) throws SQLException {

		StringBuilder inputStringBuilder = new StringBuilder("(");
		int parSize = parameter.size();
		logger.error("----------------------------procedure calling----------------------");
		logger.error("procedure name--"+procedureName);
		logger.error("parameter size  -"+parSize);
		
		for (int k = 0; k <= parSize; k++) 
		{
			inputStringBuilder.append("?,");
			logger.error("parameter   -" +k +"-"+parameter.get(String.valueOf(k + 1)));
		}
		
		String inputString = "";
		if (parSize == 0) {
			inputString = "(?)}";
		} 
		else 
		{
			inputString = inputStringBuilder.substring(0, inputStringBuilder.length() - 1)  + ")}";
		}
		
		String strProc = "{call " + procedureName + inputString;

		ResultSet rs = null;
		CallableStatement cs = null;
		try 
		{
			
		cs = con.prepareCall(strProc); 
		
			int paramSize = parSize;
			for (int i = 0; i < paramSize; i++) {
				cs.setString(i + 1, parameter.get(String.valueOf(i + 1)));

			}
			cs.registerOutParameter(paramSize + 1, OracleTypes.CURSOR);
			cs.execute();
			rs = (ResultSet) cs.getObject(paramSize + 1);

		}
		catch (SQLException e) 
		{
			throw new SQLException(e);
		}
		return rs;

	}

}
