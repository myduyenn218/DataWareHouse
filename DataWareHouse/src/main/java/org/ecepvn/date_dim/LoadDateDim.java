package org.ecepvn.date_dim;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ExtractData.ExtractData;
import config.DBConnection;
import config.OpenControlDB;

public class LoadDateDim {
	public static final String OUT_FILE = "date_dim_without_quarter.csv";
	public static Connection connectDBWH = null;

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

	public void loadDateDim() {
		File file = new File(OUT_FILE);
		if (!file.exists()) {
			new Date_Dim().date();
		}
		try {
			String sql = "Select url_db_staging,table_name_staging,url_db_warehouse,username_db , password_db FROM myconfig";
			PreparedStatement p = OpenControlDB.openControlDB().prepareStatement(sql);
			ResultSet resultSet = p.executeQuery();
			String urlStaging, urlWarehouse, tableNameStaging, tableNameWH, password, username, location;
			resultSet.next(); // đọc dòng đầu tiên

//			urlStaging = resultSet.getString("url_db_staging"); 
//			tableNameStaging = resultSet.getString("table_name_staging");
			urlWarehouse = resultSet.getString("url_db_warehouse");
			username = resultSet.getString("username_db"); // admin
			password = resultSet.getString("password_db"); // admin
			p.close();
//			connectDBStaging = DBConnection.getConnection(urlStaging, username, password);
			connectDBWH = DBConnection.getConnection(urlWarehouse, username, password);
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
			new ExtractData().loadCSV(connectDBWH, OUT_FILE, "date_dim");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		new LoadDateDim().loadDateDim();
	}
}
