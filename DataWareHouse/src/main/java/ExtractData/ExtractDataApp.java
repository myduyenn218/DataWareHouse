package ExtractData;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import config.DBConnection;
import config.ReadProperties;

public class ExtractDataApp {
	Connection CONNECTION_CONTROLLDATA;

	public void openControlDB() throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		if (CONNECTION_CONTROLLDATA == null) {
			String host = ReadProperties.getProperty("host");
			String database = ReadProperties.getProperty("database");
			String jdbcURL_1 = "jdbc:mysql://" + host + "/" + database
					+ "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
			String userName_1 = ReadProperties.getProperty("username");
			String password_1 = ReadProperties.getProperty("password");
			CONNECTION_CONTROLLDATA = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		}
	}

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

	public void startExtract() throws ClassNotFoundException, SQLException, IOException, NoSuchAlgorithmException {
		openControlDB();
		String sql = "Select url_db_staging,table_name_staging, path_file_local,username_db , password_db FROM myconfig";
		PreparedStatement p = CONNECTION_CONTROLLDATA.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery();
		String url, tableName, password, username, location;
		resultSet.next(); // đọc dòng đầu tiên

		url = resultSet.getString("url_db_staging"); // http://drive.ecepvn.org:5000/webapi
		tableName = resultSet.getString("table_name_staging");
		username = resultSet.getString("username_db"); // admin
		password = resultSet.getString("password_db"); // admin
		location = resultSet.getString("path_file_local"); // folder local
		p.close();
		System.out.println(url);
		System.out.println(username);
		System.out.println(password);
		ExtractData ex = new ExtractData();
		Connection connect = DBConnection.getConnection(url, username, password);
		String sqlLogs = "SELECT * From logs";
		PreparedStatement pre = CONNECTION_CONTROLLDATA.prepareStatement(sqlLogs);
		ResultSet re = pre.executeQuery();
		ex.existsTable(connect, tableName);
		File folder = new File(location);
		while (re.next()) {
			if (re.getString("status").equals("OK")) {
				String nameFile = re.getString("id_filename");
				for (File f : folder.listFiles()) {
					if (nameFile.equals(f.getName())) {
						String typeFile = f.getName().split("\\.")[1];
						System.out.println(f.getName());
						if (typeFile.equals("xlsx")) {
							try {
								ex.readExcel(connect, location + f.getName(), tableName);
								updatedLog(connect, "ReadyTransform", f.getName());
							} catch (Exception e) {
								// TODO: handle exception
							}
						} else if (typeFile.equals("csv")) {
							try {
								System.out.println();
								ex.loadCSV(connect, location + f.getName(), tableName);
								updatedLog(connect, "ReadyTransform", f.getName());
							} catch (Exception e) {
								// TODO: handle exception
							}
						}
					} else {
						System.out.println(f.getAbsolutePath() + ": File not exists");
					}
				}
			}

		}

	}

}
