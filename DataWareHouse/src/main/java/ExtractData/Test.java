package ExtractData;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class Test {
	//

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		ExtractData ex = new ExtractData();
//		ex.readExcel(null, null, null, null, null);
		ex.config();
		ArrayList<ArrayList<String>> dataConfig = ex.config();
		int endField = dataConfig.size() - 1;
		String jdbcURL_1 = dataConfig.get(endField).get(1); // src
		String userName_1 = dataConfig.get(endField).get(4); // username
		String password_1 = dataConfig.get(endField).get(5); // pass
		System.out.println("ok");
		String jdbcURL_2 = dataConfig.get(endField).get(2); // des
		String userName_2 = dataConfig.get(endField).get(4);
		String password_2 = dataConfig.get(endField).get(5);

		String urlFile = dataConfig.get(endField).get(6) + dataConfig.get(endField).get(3); // srcFile data: location +
																							// filename

		String fieldName = dataConfig.get(endField).get(7); // get field name db

		String delimited = dataConfig.get(endField).get(8);

		String srctableName = dataConfig.get(endField).get(11);
		String destableName = dataConfig.get(endField).get(12);
		String srcDBName = dataConfig.get(endField).get(13);
		String desDBName = dataConfig.get(endField).get(14);
		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);
		ex = new ExtractData(connectionDB1, connectionDB2);
		ex.load(delimited, urlFile, fieldName, srctableName);
		ex.copy(srcDBName, srctableName, desDBName, destableName, fieldName);
	}
}
