package config;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;

public class OpenConnection {

	public static Connection openConnectWithDBName(String dbName)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		String host = ReadProperties.getProperty("host");
//		String database = ReadProperties.getProperty("database");
		String jdbcURL_1 = "jdbc:mysql://" + host + "/" + dbName
				+ "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = ReadProperties.getProperty("username");
		String password_1 = ReadProperties.getProperty("password");
		return DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
	}
}
