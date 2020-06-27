package myduyen.Download;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import ExtractData.DBConnection;
import ExtractData.ExtractData;

/**
 * App
 */
public class App {

	public App() throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		// connect controldata
		String jdbcURL_1 = "jdbc:mysql://localhost/controldata?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "admin";
		String password_1 = "admin";
		Connection connectionDB = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);

		String sql = "Select urlDownload, userNameDownload, passwordDownload, remoteFile, locationFile, typeFile FROM myconfig";
		PreparedStatement p = connectionDB.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery();

		String url, remoteFile, location, username, password, typeFile;
		resultSet.next(); // đọc dòng đầu tiên

		url = resultSet.getString("urlDownload"); // http://drive.ecepvn.org:5000/webapi
		username = resultSet.getString("userNameDownload"); // guest_access
		password = resultSet.getString("passwordDownload"); // 123456
		remoteFile = resultSet.getString("remoteFile"); // /ECEP/song.nguyen/DW_2020/data
		location = resultSet.getString("locationFile"); // folder local
		typeFile = resultSet.getString("typeFile");
		SynologyNas nas = new SynologyNas(url, username, password);
		ArrayList<RemoteFile> filePaths = nas.list(remoteFile, 0, 0);

		sql = "SELECT updated_at FROM logs WHERE id_filename =?";
		// lấy ra từng file trong data trên server
		for (RemoteFile file : filePaths) {
			System.out.printf("Name: %s, Path: %s, isDir: %s, mTime: %s \n", file.getName(), file.getPath(),
					file.isDir(), file.getModifyTime());
			System.out.println("SQL: " + sql);
			System.out.println(file.getTypeFile() + "      " + typeFile);
			if (file.getTypeFile().equals(typeFile)) {
				System.out.println("Tải");
				PreparedStatement pre = null;
				pre = connectionDB.prepareStatement(sql);
				pre.setString(1, file.getName());
				ResultSet re = pre.executeQuery();
				re.last();
				sql = "SELECT updated_at FROM logs WHERE id_filename =?";
				// kiểm tra file đó đã được tải chưa
				if (re.getRow() == 1) {
					re.beforeFirst();
					re.next();
					Timestamp mTimeLocal = re.getTimestamp("updated_at");
					// đã tải rồi thì kiểm tra xem có cần cập nhật không bằng cách so sánh date time
					if (mTimeLocal.before(new Timestamp(file.getModifyTime()))) {
						// nếu time local trước time server thì tải về
						nas.download(file.getPath(), location + file.getName());
						System.out.println(file.getPath());
						nas.download(file.getPath(), location + file.getName());
						String inserLogs = "UPDATE logs set(status=?,myconfig=?, transform=?) where id=?";
						pre = connectionDB.prepareStatement(inserLogs);

						String md5 = nas.getMD5(file.getPath());

						if (md5.equals(nas.getDigest(location + file.getName(), 2048))) {
							System.out.println("OK");
							pre.setString(1, "OK");
							pre.setInt(2, 1);
							pre.setString(3, file.getName());
							pre.setString(4, "NotReadyTransfrom");
							pre.executeUpdate();
						} else {
							pre.setString(1, "Fails");
							pre.setInt(2, 1);
							pre.setString(3, file.getName());
							pre.setString(4, "NotReadyTransfrom");
							pre.executeUpdate();
						}
					}
				} else {
					// xử lý file chưa được tải
					String inserLogs = "INSERT INTO logs (id_filename, myconfig_id,status,transform  ) VALUES (?,?,?,?)";
					pre = connectionDB.prepareStatement(inserLogs);
					nas.download(file.getPath(), location + file.getName());

					String md5 = nas.getMD5(file.getPath());

					if (md5.equals(nas.getDigest(location + file.getName(), 2048))) {
						pre.setString(1, file.getName());
						pre.setInt(2, 1);
						pre.setString(3, "OK");
						pre.setString(4, "ReadyTransfrom");
						pre.executeUpdate();
					} else {
						pre.setString(1, file.getName());
						pre.setInt(2, 1);
						pre.setString(3, "Fails");
						pre.setString(4, "NotReadyTransfrom");
						pre.executeUpdate();
					}

				}

			}
		}
	}

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {

		new App();

	}
}