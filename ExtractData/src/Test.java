import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class Test {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String jdbcURL_1 = "jdbc:mysql://localhost/datawarehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "root";
		String password_1 = "";

		String jdbcURL_2 = "jdbc:mysql://localhost/datacopy?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_2 = "root";
		String password_2 = "";

		String urlFile = "information.csv";

		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);

		ExtractData ex = new ExtractData(connectionDB1, connectionDB2);

		ex.load(",", urlFile);
		ex.copy("datawarehouse", "information", "datacopy", "info");
	}
}
