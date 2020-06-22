package myduyen.Download;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import ExtractData.DBConnection;
import ExtractData.ExtractData;

/**
 * App
 */
public class App {

	public App() throws ClassNotFoundException, SQLException {
		ExtractData ex = new ExtractData();
		String jdbcURL_1 = "jdbc:mysql://localhost/controldata?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "admin";
		String password_1 = "admin";
		Connection connectionDB = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);

		String sql = "Select * FROM downloadconfig WHERE id = 1";

		PreparedStatement p = connectionDB.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery(sql);
		ResultSetMetaData rsmd = resultSet.getMetaData();
//		int columnsNumber = rsmd.getColumnCount();

		String url, remoteFile, location, username, password;
		resultSet.next(); // đọc dòng đầu tiên

		url = resultSet.getString("url");
		username = resultSet.getString("username");
		password = resultSet.getString("password");
		remoteFile = resultSet.getString("remoteFile");
		location = resultSet.getString("location");

		String inserLogs = "INSERT INTO logs (id_filename, myconfig_id,status,transform  ) VALUES (?,?,?,?)";
		PreparedStatement pre = connectionDB.prepareStatement(inserLogs);
		SynologyNas nas = new SynologyNas(url, username, password);
		ArrayList<RemoteFile> filePaths = nas.list(remoteFile, 0, 0);
		for (RemoteFile file : filePaths) {
//			System.out.printf("Name: %s, Path: %s, isDir: %s\n", file.getName(), file.getPath(), file.isDir());
			if (nas.download(remoteFile, location + file.getName())) {
				pre.setString(1, file.getName());
				pre.setInt(2, 1);
				pre.setString(3, "OK");
				pre.setString(4, "ReadyTransfrom");
				pre.execute();
			} else {
				pre.setString(1, file.getName());
				pre.setInt(2, 1);
				pre.setString(3, "Fails");
				pre.setString(4, "NotReadyTransfrom");
				pre.execute();
			}
		}

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		new App();

	}
}