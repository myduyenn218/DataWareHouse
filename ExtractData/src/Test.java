import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class Test {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String jdbcURL_1 = "jdbc:mysql://localhost/dw-extractdb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "admin";
		String password_1 = "admin";

		String jdbcURL_2 = "jdbc:mysql://localhost/dw-extractdb-2?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_2 = "admin";
		String password_2 = "admin";

		String urlFile = "information.csv";

		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);

		ExtractData ex = new ExtractData(connectionDB1, connectionDB2);

		ex.load(",", urlFile);
		ex.copy("datawarehouse", "information", "dw-extractdb-2", "info");
	}
}
