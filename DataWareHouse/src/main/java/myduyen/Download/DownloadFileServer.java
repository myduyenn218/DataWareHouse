package myduyen.Download;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ExtractData.DBConnection;

/**
 * App
 */
public class DownloadFileServer {
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

	public void closeControlDB() throws SQLException {
		if (CONNECTION_CONTROLLDATA != null) {
			CONNECTION_CONTROLLDATA.close();
		}
	}

	private void insertLog(String status, int configId, String transform, String id) {
		String query = "INSERT INTO logs (status, myconfig, transform, id_filename) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE status=?, myconfig=?, transform=?";
		PreparedStatement pre;
		try {
			pre = CONNECTION_CONTROLLDATA.prepareStatement(query);
			pre.setString(1, status);
			pre.setInt(2, configId);
			pre.setString(3, transform);
			pre.setString(4, id);
			pre.setString(5, status);
			pre.setInt(6, configId);
			pre.setString(7, transform);
			pre.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean downloadFile(SynologyNas nas, String remotePath, String localPath) {
		nas.download(remotePath, localPath);

		try {
			String remoteMd5 = nas.getMD5(remotePath);
			String localMd5 = CryptoHelper.calculateMD5(localPath, 2048);
			return remoteMd5.equals(localMd5);
		} catch (NoSuchAlgorithmException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	public void run() throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		openControlDB();

		String sql = "Select id, url_download, username_download, password_download, remote_file, path_file_local, type_file FROM myconfig";
		PreparedStatement p = CONNECTION_CONTROLLDATA.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery();
		String url, remoteFile, location, username, password, typeFile;
		resultSet.next(); // đọc dòng đầu tiên

		int id = Integer.parseInt(resultSet.getString("id"));
		url = resultSet.getString("url_download"); // http://drive.ecepvn.org:5000/webapi
		username = resultSet.getString("username_download"); // guest_access
		password = resultSet.getString("password_download"); // 123456
		remoteFile = resultSet.getString("remote_file"); // /ECEP/song.nguyen/DW_2020/data
		location = resultSet.getString("path_file_local"); // folder local
		typeFile = resultSet.getString("type_file");
		p.close();
		SynologyNas nas = new SynologyNas(url, username, password);
		ArrayList<RemoteFile> filePaths = nas.list(remoteFile, 0, 0);

		sql = "SELECT updated_at FROM logs WHERE id_filename =?";
		PreparedStatement pre = null;
		// lấy ra từng file trong data trên server

		for (RemoteFile file : filePaths) {
			System.out.printf("Name: %s, Path: %s, isDir: %s, mTime: %s \n", file.getName(), file.getPath(),
					file.isDir(), file.getModifyTime());
			System.out.println("SQL: " + sql);

			String[] type_file = typeFile.split(",");
			for (String type : type_file) {
				if (file.getTypeFile().equals(type)) {
					System.out.println("Tải");
					pre = CONNECTION_CONTROLLDATA.prepareStatement(sql);
					pre.setString(1, file.getName());
					ResultSet re = pre.executeQuery();
					re.last();

					// kiểm tra file đó đã được tải chưa
					if (re.getRow() == 1) {
						re.beforeFirst();
						re.first();
						Timestamp mTimeLocal = re.getTimestamp("updated_at");
						// đã tải rồi thì kiểm tra xem có cần cập nhật không bằng cách so sánh date time
						if (mTimeLocal.before(new Timestamp(file.getModifyTime()))) {
							boolean isFileDownloaded = downloadFile(nas, file.getPath(), location + file.getName());

							if (isFileDownloaded) {
								insertLog("OK", id, "NotReadyTransfrom", file.getName());
							} else {
								insertLog("Fails", id, "NotReadyTransfrom", file.getName());
							}
						}
					} else {
						boolean isFileDownloaded = downloadFile(nas, file.getPath(), location + file.getName());

						if (isFileDownloaded) {
							insertLog("OK", id, "NotReadyTransfrom", file.getName());
						} else {
							insertLog("Fails", id, "NotReadyTransfrom", file.getName());
						}
					}
					pre.close();
				}

			}

		}
//		closeControlDB(); bug sql khoong dongs duocjw
	}

	public DownloadFileServer() throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		ses.scheduleAtFixedRate(new Runnable() {
		    @Override
		    public void run() {
		      run();
		    }
		}, 0, 1, TimeUnit.HOURS);
	}

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		new DownloadFileServer();

	}
}