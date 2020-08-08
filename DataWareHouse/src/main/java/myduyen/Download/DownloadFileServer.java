package myduyen.Download;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import config.DBConnection;
import config.OpenConnection;
import config.ReadProperties;
import config.SendMail;
import synologynas.MD5Exception;
import synologynas.RemoteFile;
import synologynas.SynologyNas;
import synologynas.exception.ListFileException;
import synologynas.exception.LoginException;

/**
 * App
 */
public class DownloadFileServer {
	Connection CONNECTION_CONTROLLDATA;

	public void openControlDB() throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		if (CONNECTION_CONTROLLDATA == null) {
			CONNECTION_CONTROLLDATA = OpenConnection.openConnectWithDBName("controldata");
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
		try {
			nas.download(remotePath, localPath);
		} catch (LoginException e1) {
			SendMail.sendMail("Thông tin debug data", e1.getMessage(), "Đăng nhập thất bại!");
			e1.printStackTrace();
		}

		try {
			String remoteMd5 = nas.getMD5(remotePath);
			String localMd5 = CryptoHelper.calculateMD5(localPath, 2048);
			return remoteMd5.equals(localMd5);
		} catch (NoSuchAlgorithmException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MD5Exception e) {
			SendMail.sendMail("Thông tin debug data", e.getMessage(), "Lấy MD5 trên server thất bại!");
			e.printStackTrace();
		} catch (LoginException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	public void run(String idConfig)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		// 1.connect controldata
		openControlDB();
		// 2. Select dữ liệu trong table myconfig:
		String sql = "Select url_download, username_download, password_download, remote_file, path_file_local, type_file, format FROM myconfig WHERE id=?";
		PreparedStatement p = CONNECTION_CONTROLLDATA.prepareStatement(sql);
		p.setString(1, idConfig);
//		3. Nhận được ResultSet chứa record thỏa yêu cầu truy xuất
		ResultSet resultSet = p.executeQuery();
		String url, remoteFile, location, username, password, typeFile, format;
//		4. Duyệt ResultSet 
		resultSet.next(); // đọc dòng đầu tiên

		int id = Integer.parseInt(idConfig);
		url = resultSet.getString("url_download"); // http://drive.ecepvn.org:5000/webapi
		username = resultSet.getString("username_download"); // guest_access
		password = resultSet.getString("password_download"); // 123456
		remoteFile = resultSet.getString("remote_file"); // /ECEP/song.nguyen/DW_2020/data
		location = resultSet.getString("path_file_local"); // folder local
		typeFile = resultSet.getString("type_file");
		format = resultSet.getString("format");
		p.close();
		// select time download của file đã được download trước đó
		sql = "SELECT time_download FROM logs WHERE id_filename =?";
		PreparedStatement pre = null;

		SynologyNas nas = new SynologyNas(url, username, password);
//		5. Kết nối Synology Nas: login() trong method list có kiểm tra login chưa. chưa login thì login
//		6. Gọi hàm list(final String folder, final int offset, final int limit) trả về danh sách file trong folder

		ArrayList<RemoteFile> filePaths = null;
		try {
			filePaths = nas.list(remoteFile, 0, 0);
		} catch (LoginException e) {
//			Gửi thông báo lỗi về mail
			SendMail.sendMail("Thông tin debug data", e.getMessage(), "Đăng nhập thất bại!");
			e.printStackTrace();
		} catch (ListFileException e) {
			SendMail.sendMail("Thông tin debug data", e.getMessage(), "Lấy ra danh sách file thất bại!");
			e.printStackTrace();
		}

//		7. Duyệt từng file
		for (RemoteFile file : filePaths) {
			String[] type_file = typeFile.split(",");
			// tải những loại file mà database cho phép
			for (String type : type_file) {
				// và kiểm tra tên file theo đúng định dạng
				if (file.getTypeFile().equals(type) && file.getName().toLowerCase().contains(format)) {
//					System.out.println("Tải");
					pre = CONNECTION_CONTROLLDATA.prepareStatement(sql);
					pre.setString(1, file.getName());
					ResultSet re = pre.executeQuery();
					re.last();

					// kiểm tra file đó đã được tải chưa
					if (re.getRow() == 1) {
						re.beforeFirst();
						re.first();
						Timestamp mTimeLocal = re.getTimestamp("time_download");
						// đã tải rồi thì kiểm tra xem có cần cập nhật không bằng cách so sánh date time
						if (mTimeLocal.before(new Timestamp(file.getModifyTime()))) {
//							8. Download File: downloadFile(SynologyNas nas, String remotePath, String localPath) 
							boolean isFileDownloaded = downloadFile(nas, file.getPath(), location + file.getName());

							if (isFileDownloaded) {
								insertLog("OK", id, "NotReadyTransfrom", file.getName());
							} else {
								insertLog("Fails", id, "NotReadyTransfrom", file.getName());
							}
							System.out.printf("Name: %s, Path: %s, isDir: %s, mTime: %s \n", file.getName(),
									file.getPath(), file.isDir(), file.getModifyTime());
//							System.out.println("SQL: " + sql);
						}
					} else {
						boolean isFileDownloaded = downloadFile(nas, file.getPath(), location + file.getName());
//						9, Update, Insert Logs:
//							insertLog(String status, int configId, String transform, String id)
						if (isFileDownloaded) {
							insertLog("OK", id, "NotReadyTransfrom", file.getName());
						} else {
							insertLog("Fails", id, "NotReadyTransfrom", file.getName());
						}
						System.out.printf("Name: %s, Path: %s, isDir: %s, mTime: %s \n", file.getName(), file.getPath(),
								file.isDir(), file.getModifyTime());
//						System.out.println("SQL: " + sql);
					}
//					10. Đóng tất cả kết nối:
//						closeControlDB()
					pre.close();
				}

			}

		}
//		closeControlDB(); bug sql khoong dongs duocjw
	}


}