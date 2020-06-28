package ExtractData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	public static Connection getConnection(String jdbcURL, String userName, String password)
			throws ClassNotFoundException, SQLException {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);

		return connection;

	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		String jdbcURL = "jdbc:mysql://localhost/dssv?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName ="root";
		String password = "";
		Connection connection = DBConnection.getConnection(jdbcURL, userName, password);
		if(connection != null) {
			
		}
	}
}
