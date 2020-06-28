package ExtractData;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Test {
	private void updatedLog(Connection con, String transform, String id) {
		String query = "UPDATE logs SET transform =? WHERE id_filename = ?";
		PreparedStatement pre;
		try {
			pre = con.prepareStatement(query);
			pre.setString(1, transform);
			pre.setString(2, id);
			pre.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Test() throws ClassNotFoundException, SQLException, IOException {

		ExtractData ex = new ExtractData();
		String path = "/home/myduyen/Desktop/Data";
		File folder = new File(path);
		Connection connect = DBConnection.getConnection(
				"jdbc:mysql://localhost/datacopy?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
				"admin", "admin");
		Connection connectControldata = DBConnection.getConnection(
				"jdbc:mysql://localhost/controldata?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
				"admin", "admin");
		String sqlLogs = "SELECT * From logs";
		PreparedStatement pre = connectControldata.prepareStatement(sqlLogs);
		ResultSet re = pre.executeQuery();
		ex.existsTable(connect, "Sinhvien");
		while (re.next()) {
			if (re.getString("status").equals("OK")) {
				String nameFile = re.getString("id_filename");
				for (File f : folder.listFiles()) {
					if (nameFile.equals(f.getName())) {
						String typeFile = f.getName().split("\\.")[1];
						System.out.println(f.getName());
						if (typeFile.equals("xlsx")) {
							try {
								ex.readExcel(connect, "/home/myduyen/Desktop/Data/" + f.getName(), "Sinhvien");
								updatedLog(connect, "ReadyTransform", f.getName());
							} catch (Exception e) {

								// TODO: handle exception
							}

						} else if (typeFile.equals("csv")) {
							try {
								ex.loadCSV(connect, "/home/myduyen/Desktop/Data/" + f.getName(), "Sinhvien");
								updatedLog(connect, "ReadyTransform", f.getName());
							} catch (Exception e) {
								// TODO: handle exception
							}
						}
					} else {
						System.out.println("File not exists");
					}
				}
			}

		}
	}

	//

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		new Test();
//		ex.readExcel(null, null, null, null, null);
//		ex.config();
//		ArrayList<ArrayList<String>> dataConfig = ex.config();
//		int endField = dataConfig.size() - 1;
//		String jdbcURL_1 = dataConfig.get(endField).get(1); // src
//		String userName_1 = dataConfig.get(endField).get(4); // username
//		String password_1 = dataConfig.get(endField).get(5); // pass
//		System.out.println("ok");
//		String jdbcURL_2 = dataConfig.get(endField).get(2); // des
//		String userName_2 = dataConfig.get(endField).get(4);
//		String password_2 = dataConfig.get(endField).get(5);
//
//		String urlFile = dataConfig.get(endField).get(6) + dataConfig.get(endField).get(3); // srcFile data: location +
//																							// filename
//
//		String fieldName = dataConfig.get(endField).get(7); // get field name db
//
//		String delimited = dataConfig.get(endField).get(8);
//
//		String srctableName = dataConfig.get(endField).get(11);
//		String destableName = dataConfig.get(endField).get(12);
//		String srcDBName = dataConfig.get(endField).get(13);
//		String desDBName = dataConfig.get(endField).get(14);
//		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
//		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);
//		ex = new ExtractData(connectionDB1, connectionDB2);
//		ex.load(delimited, urlFile, fieldName, srctableName);
//		ex.copy(srcDBName, srctableName, desDBName, destableName, fieldName);
	}
}
