package dao;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.DBConnection;
import config.OpenConnection;

public class ControlDB {
	private String config_db_name;
	private String target_db_name;
	private String table_name;
	private PreparedStatement pst = null;
	private ResultSet rs = null;
	private String sql;

	public ControlDB(String db_name, String table_name, String target_db_name) {
		this.config_db_name = db_name;
		this.table_name = table_name;
		this.target_db_name = target_db_name;
	}

	public ControlDB() {
	}

	// Sua:
	public ControlDB(String target_db_name) {
		this.target_db_name = target_db_name;
	}

	public String getConfig_db_name() {
		return config_db_name;
	}

	public void setConfig_db_name(String config_db_name) {
		this.config_db_name = config_db_name;
	}

	public String getTarget_db_name() {
		return target_db_name;
	}

	public void setTarget_db_name(String target_db_name) {
		this.target_db_name = target_db_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	// Phuong thuc lay cac thuoc tinh co trong bang config:
	public List<Config> loadAllConfs(int condition)
			throws SQLException, ClassNotFoundException, NoSuchAlgorithmException, IOException {
		List<Config> listConfig = new ArrayList<Config>();
		// kết nổi database controldata
		Connection conn = OpenConnection.openConnectWithDBName("controldata");
		// select các trường của config
		String selectConfig = "select * from myconfig where id=?";
		// thực hiện câu sql
		PreparedStatement ps = conn.prepareStatement(selectConfig);
		
		ps.setInt(1, condition);
		ResultSet rs = ps.executeQuery();

		// lấy tất cả các trường cho vào list
		while (rs.next()) {
			Config conf = new Config();
			conf.setIdConf(rs.getInt("id"));
			conf.setConfigName(rs.getString("configName"));
			conf.setUrl_db_staging(rs.getString("url_db_staging"));
			conf.setTable_name_staging(rs.getString("table_name_staging"));
			conf.setUrl_db_warehouse(rs.getString("url_db_warehouse"));
			conf.setTable_name_warehouse(rs.getString("table_name_warehouse"));
			conf.setPath_file_local(rs.getString("path_file_local"));
			conf.setUsername_download(rs.getString("username_download"));
			conf.setPassword_download(rs.getString("password_download"));
			conf.setRemote_file(rs.getString("remote_file"));
			conf.setType_file(rs.getString("type_file"));
			conf.setUsername_db(rs.getString("username_db"));
			conf.setPassword_db(rs.getString("password_db"));
			conf.setFields(rs.getString("fields"));
			listConfig.add(conf);
		}
		return listConfig;
	}

	// Phuong thuc lay cac thuoc tinh co trong bang log:
	public Log getLogsWithStatus(String condition, int idconfig)
			throws SQLException, ClassNotFoundException, NoSuchAlgorithmException, IOException {
		// List<Log> listLog = new ArrayList<Log>();
		Log log = new Log();
		// thực hiện kết nối controldata
		Connection conn = OpenConnection.openConnectWithDBName("controldata");
		// thực hiện lấy các trường trong logs ứng với id_config và stastus
		String selectLog = "select * from logs where status=? and myconfig =?";
		// thực hiện câu query
		PreparedStatement ps = conn.prepareStatement(selectLog);
		
		ps.setString(1, condition);
		ps.setInt(2, idconfig);

		ResultSet rs = ps.executeQuery();
		rs.last();
		if (rs.getRow() >= 1) {
			rs.first();
			log.setIdLog(rs.getString("id_filename"));
			log.setIdConfig(rs.getInt("myconfig"));
			log.setStatus(rs.getString("status"));
			log.setResult(rs.getString("transform"));
//			log.setNumColumn(rs.getInt("numColumn"));
//			log.setFileName(rs.getString("fileName"));
		}
//		System.out.println(log.toString());
		return log;
	}

	// Chen du lieu vao bang trong database staging:
	public boolean insertValues(String fieldName, String values, String targetTable)
			throws ClassNotFoundException, NoSuchAlgorithmException, IOException {
		// khai báo câu lệnh sql insert vào table cần insert
		sql = "INSERT INTO " + targetTable + "(" + fieldName + ") VALUES " + values;
//		System.out.println(sql);
		System.out.println("Start load file: " + fieldName);
		try {
			// mở kết nối với staging sau đó thực hiện câu lệnh sql
			pst = OpenConnection.openConnectWithDBName(this.target_db_name).prepareStatement(sql);
			//  kết quả update bảng staging vừa load vào
			pst.executeUpdate();
			// nếu thành công thông báo thành công
			System.out.println("Load file successfully");
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			// không thành công thông báo thất bại
			System.out.println("Load file fails");
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				System.out.println("Load file fails");
				e.printStackTrace();
			}

			
		}
	}

	// Chen du lieu vao log:
//	public boolean insertLog(String table, String file_status, int config_id, String timestamp,
//			String stagin_load_count, String file_name)
//			throws ClassNotFoundException, NoSuchAlgorithmException, IOException {
//		sql = "INSERT INTO " + table + "(id_filename,idConfig,state,dateUserInsert) value (?,?,?,?)";
//		try {
//			pst = OpenConnection.openConnectWithDBName(this.config_db_name).prepareStatement(sql);
//			pst.setString(1, file_name);
//			pst.setInt(2, config_id);
//			pst.setString(3, file_status);
//	
//			pst.setString(4, timestamp);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
	//phương thức kiểm tra bảng có tồn tại hay ko
		public int checkTableExist(Connection connection, String target_Table, String target_db_name) throws ClassNotFoundException, NoSuchAlgorithmException, IOException {
			String sql = "SELECT COUNT(*)\r\n" + "FROM information_schema.tables \r\n" + "WHERE table_schema = '" + target_db_name
					+ "' \r\n" + "AND table_name = '" + target_Table + "';";
			PreparedStatement statement = null;
			ResultSet res = null;
			try {
				pst = OpenConnection.openConnectWithDBName(this.target_db_name).prepareStatement(sql);
				ResultSet rs = pst.executeQuery();
				while (rs.next()) {
					return rs.getInt(1);
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					// khác null nghĩa là đã chạy rồi nên đóng kết nối
					if (res != null) {
						res.close();
					}
					if (statement != null) {
						statement.close();
					}

				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return 0;
		}

	// Upload logs sau khi thực hiện load vào staging
	public boolean updateLogAfterLoadToStaging(String status, String result, String fileTimeStamp, String fileName)
			throws ClassNotFoundException, NoSuchAlgorithmException, IOException {
		Connection connection;
		//khai báo câu query upload
		String sql = "UPDATE logs SET status=?, transform=?, dateLoadToStaging=? WHERE id_fileName=?";
		try {
			// kết nối với ontroldata
			connection = OpenConnection.openConnectWithDBName("controldata");
			// thực hiện câu sql
			PreparedStatement ps1 = connection.prepareStatement(sql);
			//với 
			ps1.setString(1, status);
			ps1.setString(2, result);
			ps1.setString(3, fileTimeStamp);
			ps1.setString(4, fileName);
			ps1.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	public void truncateTable(Connection connection, String table_name) {
		PreparedStatement statementTruncate;
		try {
			statementTruncate = connection.prepareStatement("TRUNCATE TABLE " + table_name);
			statementTruncate.execute();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("TRUNCATED " + table_name);
	}

//	// Tao bang:
//	public boolean createTable(String table_name, String variables, String column_list) throws ClassNotFoundException {
//		sql = "CREATE TABLE " + table_name + " (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
//		String[] vari = variables.split(",");
//		String[] col = column_list.split(",");
//		for (int i = 0; i < vari.length; i++) {
//			sql += col[i] + " " + vari[i] + " NOT NULL,";
//		}
//		sql = sql.substring(0, sql.length() - 1) + ")";
//		System.out.println(sql);
//		try {
//			pst = DBConnection.getConnection(this.target_db_name).prepareStatement(sql);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}

	// Phuong thuc loadInFile() load file vao trong table:
	public int loadInFile(String sourceFile, String targetTable, String delimeter)
			throws SQLException, ClassNotFoundException, NoSuchAlgorithmException, IOException {
		sql = "LOAD DATA LOCAL INFILE '" + sourceFile + "' INTO TABLE " + targetTable + "\r\n"
				+ "FIELDS TERMINATED BY '" + delimeter + "' \r\n" + "ENCLOSED BY '\"' \r\n"
				+ "LINES TERMINATED BY '\r\n'";
		Connection conn = OpenConnection.openConnectWithDBName(this.target_db_name);
		PreparedStatement pst = conn.prepareStatement(sql);
		System.out.println("LOAD DATA LOCAL INFILE '" + sourceFile + "' INTO TABLE " + targetTable + "\r\n"
				+ "FIELDS TERMINATED BY '" + delimeter + "' \r\n" + "LINES TERMINATED BY '\\n'" + " IGNORE 1 ROWS");
		return pst.executeUpdate();

	}

}
