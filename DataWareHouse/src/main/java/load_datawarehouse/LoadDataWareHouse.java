package load_datawarehouse;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import config.DBConnection;
import config.ReadProperties;

public class LoadDataWareHouse {
	Connection CONNECTION_CONTROLLDATA;
	Connection connectDBStaging;
	Connection connectDBWH;

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

	public void connectDB() throws ClassNotFoundException, NoSuchAlgorithmException, SQLException, IOException {
		openControlDB();
		String sql = "Select url_db_staging,table_name_staging,url_db_warehouse, table_name_warehouse,username_db , password_db FROM myconfig";
		PreparedStatement p = CONNECTION_CONTROLLDATA.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery();
		String urlStaging, urlWarehouse, tableNameStaging, tableNameWH, password, username, location;
		resultSet.next(); // đọc dòng đầu tiên

		urlStaging = resultSet.getString("url_db_staging"); // http://drive.ecepvn.org:5000/webapi
		tableNameStaging = resultSet.getString("table_name_staging");
		urlWarehouse = resultSet.getString("url_db_warehouse");
		tableNameWH = resultSet.getString("table_name_warehouse");
		username = resultSet.getString("username_db"); // admin
		password = resultSet.getString("password_db"); // admin
		p.close();
		connectDBStaging = DBConnection.getConnection(urlStaging, username, password);
		connectDBWH = DBConnection.getConnection(urlWarehouse, username, password);
	}

	@SuppressWarnings("deprecation")
	private Date convertDate(String dateStr) {
		System.out.println(dateStr);
		if (dateStr.contains("/")) {
			String temp[] = dateStr.split("\\/");
			return new Date(Date.UTC(Integer.parseInt(temp[2]) - 1900, Integer.parseInt(temp[1]), Integer.parseInt(temp[0]), 0, 0, 0));
		}
		return new Date(0);
	}

	private boolean isTableExist(String tableName) {
		try {
			DatabaseMetaData dbm = connectDBWH.getMetaData();
			ResultSet tables = dbm.getTables(null, null, tableName, null);
			System.out.println(tables.next());
			System.out.println(tables.getRow());
			return tables.next();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	public void copy(String database1, String nameTableDB1, String database2, String nameTableDB2, String fieldName)
			throws ClassNotFoundException, SQLException {

		if (!isTableExist("sinhvien")) {
//			System.out.println("aaaa");
			final String sql = "CREATE TABLE `warehousedata`.`sinhvien`  (\n"
					+ "  `STT` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,\n" + "  `MSSV` varchar(10) NOT NULL,\n"
					+ "  `ho` varchar(50) NOT NULL,\n" + "  `ten` varchar(10) NOT NULL,\n"
					+ "  `ngaysinh` date NOT NULL,\n" + "  `malop` varchar(10) NOT NULL,\n"
					+ "  `tenlop` varchar(50) NOT NULL,\n" + "  `dienthoai` varchar(12) NOT NULL,\n"
					+ "  `email` varchar(255) NOT NULL,\n" + "  `quequan` varchar(255) NOT NULL,\n"
					+ "  `ghichu` varchar(255) NOT NULL,\n" + "  PRIMARY KEY (`STT`)\n" + ");";
			connectDBWH.prepareStatement(sql).execute();
		}
		
		ResultSet rs;
		Statement stmt = connectDBStaging.createStatement();
		String sqlSelectNameColumn = "SELECT * FROM " + nameTableDB1;
		System.out.println(sqlSelectNameColumn);
		rs = stmt.executeQuery(sqlSelectNameColumn);

		final String insertSQl = "INSERT INTO sinhvien(MSSV, ho, ten, ngaysinh, malop, tenlop, dienthoai, email, quequan, ghichu) VALUES(?,?,?,?,?,?,?,?,?,?);";
		while (rs.next()) {
			String mssv = rs.getString("Mssv");
			String lastname = rs.getString("Hlt");
			String firstname = rs.getString("Tn");
			Date birthday = convertDate(rs.getString("Ngysinh"));
			String classCode = rs.getString("Mlp");
			String className = rs.getString("Tnlp");
			String phone = rs.getString("Tlinlc");
			String email = rs.getString("Email");
			String address = rs.getString("QuQun");
			String note = rs.getString("Ghich");
			
			final PreparedStatement p = connectDBWH.prepareStatement(insertSQl);
			p.setString(1, mssv);
			p.setString(2, lastname);
			p.setString(3, firstname);
			p.setDate(4, birthday);
			p.setString(5, classCode);
			p.setString(6, className);
			p.setString(7, phone);
			p.setString(8, email);
			p.setString(9, address);
			p.setString(10, note);
			p.execute();
		}

//		
//		
//		
//		for (int loop = 1; loop <= counter; loop++) {
//			colName[loop - 1] = md.getColumnLabel(loop);
////	sqlCreateTable += colName[loop - 1] + " CHAR(50),";
//		}
//		String sql = "";
//		String fields[] = fieldName.split(",");
//// tạo table với table name được truyền vào
//		sql = "CREATE table " + nameTableDB2 + " (";
//		for (int i = 0; i < fields.length - 1; i++) {
//			sql += fields[0] + " CHAR(50),";
//		}
//		sql += fields[0] + " CHAR(50))";
//		System.out.println(sql);
//
//		String sqlCreateTable = "CREATE table " + nameTableDB2 + "(" + colName[0] + " VARCHAR(15)," + colName[1]
//				+ " CHAR(50)," + colName[2] + " CHAR(50)," + colName[3] + " CHAR(50)," + colName[4] + " CHAR(50))";
////sqlCreateTable += ")";
//
//		System.out.println(sqlCreateTable);
//		PreparedStatement p = connectionDB2.prepareStatement(sqlCreateTable);
//		p.execute();
//
////COPY 
//		String insert = "INSERT INTO " + database2 + "." + nameTableDB2 + " SELECT * FROM " + database1 + "."
//				+ nameTableDB1;
//		System.out.println(insert);
//		PreparedStatement pc = connectionDB2.prepareStatement(insert);
//		pc.execute();
	}
	
	public static void main(String[] args) {
		LoadDataWareHouse loader = new LoadDataWareHouse();
		try {
			loader.connectDB();
			loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien", "");
		} catch (ClassNotFoundException | NoSuchAlgorithmException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
