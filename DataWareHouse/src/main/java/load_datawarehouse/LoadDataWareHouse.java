package load_datawarehouse;

import java.io.File;
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

import org.ecepvn.date_dim.Date_Dim;

import com.mysql.cj.jdbc.result.ResultSetMetaData;

import ExtractData.ExtractData;
import config.DBConnection;
import config.OpenControlDB;
import config.ReadProperties;

public class LoadDataWareHouse {
	Connection CONNECTION_CONTROLLDATA;
	Connection connectDBStaging;
	Connection connectDBWH;
	public static final String OUT_FILE = "date_dim_without_quarter.csv";
	public static final String DATA_MONHOC_FILE_2013 = "data_monhoc_2013.csv";
	public static final String DATA_MONHOC_FILE_2014 = "data_monhoc_2014.csv";

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
			return new Date(Date.UTC(Integer.parseInt(temp[2]) - 1900, Integer.parseInt(temp[1]),
					Integer.parseInt(temp[0]), 0, 0, 0));
		}
		return new Date(0);
	}

	private boolean isTableExist(String tableName) {
		try {
			DatabaseMetaData dbm = connectDBWH.getMetaData();
			ResultSet tables = dbm.getTables(null, null, tableName, null);
//			System.out.println(tableName + ": " + tables.next());
			boolean result = tables.next();
//			System.out.println(tables.getRow());
			return result;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}

		return false;
	}

	public void copy(String database1, String nameTableDB1, String database2, String nameTableDB2)
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

	}

	public void loadDateDim() {
		File file = new File(OUT_FILE);
		if (!file.exists()) {
			new Date_Dim().date();
		}
		try {
			if (!isTableExist("date_dim")) {
				final String sqlCreateTable = "create table date_dim(Date_SK BIGINT PRIMARY KEY AUTO_INCREMENT ,\n"
						+ " Full_date date null,\n" + "  DAY_SINCE_2005 int null, \n"
						+ "  Month_since_2005 int null, \n" + "  Day_Of_Week text null,\n"
						+ "  CALENDAR_MONTH text null,\n" + "  CALENDAR_YEAR int null,\n"
						+ "  Calendar_Year_Month text null,\n" + "  Day_OF_Month int null,\n"
						+ "  Day_of_year int null,\n" + "  week_of_year_sunday int null,\n"
						+ "  year_week_sunday text null,\n" + "  WEEK_SUNDAY_START text null,\n"
						+ "  WEEK_OF_YEAR_MONDAY text null,\n" + "  YEAR_WEEK_MONDAY text null,\n"
						+ "  WEEK_MONDAY_START text null,\n" + "  HOLIDAY text null,\n" + "  DAY_TYPE text null,\n"
						+ "  QUARTER_OF_YEAR text null, \n" + "  QUARTER_SINCE_2005 text null\n" + "  );";
				connectDBWH.prepareStatement(sqlCreateTable).execute();
			}
			new ExtractData().loadCSV(connectDBWH, OUT_FILE, "date_dim", "" );
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void loadMonHoc(Connection connect, String pathFile, String tableName, String fields) {
		File file = new File(DATA_MONHOC_FILE_2013);
		if (!file.exists()) {
			System.out.println("File khoong tofn tai");
			return;
		}
		try {
			System.out.println(isTableExist("mon_hoc"));
			if (!isTableExist("mon_hoc")) {
				final String sqlCreateTable = "create table mon_hoc(ID BIGINT PRIMARY KEY AUTO_INCREMENT, STT int null, Ma_MH int null, Ten_MH text null, TC int null, Khoa_BoMon_QuanLy text null, Khoa_BoMon_DangSuDung text null,Note text null, dt_expire date DEFAULT '9999-12-31');";
				connectDBWH.prepareStatement(sqlCreateTable).execute();
			}
			System.out.println("loading..");
			// test truyền vào danh sách các field muốn insert
			new ExtractData().loadCSV(connect, pathFile, tableName, fields);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		LoadDataWareHouse loader = new LoadDataWareHouse();
		try {
			loader.connectDB();
//			loader.loadDateDim();
//			loader.loadMonHoc();
//			loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien", "");
		} catch (ClassNotFoundException | NoSuchAlgorithmException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
