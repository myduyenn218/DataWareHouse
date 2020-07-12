package ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import config.DBConnection;

public class ExtractData {

	public ExtractData() {
		// TODO Auto-generated constructor stub
	}

	public boolean existsTable(Connection con, String tableName) {
//		Connection con;
		try {
//			con = DBConnection.getConnection(des_db, user_sr, password);
			DatabaseMetaData dbm = con.getMetaData();
			// check if "employee" table is there
			ResultSet tables = dbm.getTables(null, null, tableName, null);
			if (tables.next()) {
				// Table exists
				System.out.println("Table exists");

				return true;
			} else {
				String creatTable = "CREATE TABLE " + tableName
						+ "(STT BIGINT PRIMARY KEY AUTO_INCREMENT, Mssv VARCHAR(255),Hlt VARCHAR(255),Tn VARCHAR(255),Ngysinh VARCHAR(255),Mlp VARCHAR(255),Tnlp VARCHAR(255),Tlinlc VARCHAR(255),Email VARCHAR(255),QuQun VARCHAR(255),Ghich VARCHAR(255))";
				PreparedStatement preparedStatement = con.prepareStatement(creatTable);
				preparedStatement.execute();
				System.out.println("Create table Successfully :)");
				return false;
				// Table does not exist
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public void readExcel(Connection connect, String excelFile, String tableName)
			throws ClassNotFoundException, SQLException, IOException {
//		Connection connect = DBConnection.getConnection(des_db, user_sr, password);
		System.out.println("Connect DB Successfully 🙂");
		Workbook excel = new XSSFWorkbook(excelFile);
		Sheet sheet = excel.getSheetAt(0);
		Row row;
		Cell cell;
		StringBuffer stb;
		int i = 1, j = 10;

		stb = new StringBuffer("INSERT INTO ");
		stb.append(tableName);
		stb.append("(Mssv,Hlt,Tn,Ngysinh,Mlp,Tnlp,Tlinlc,Email,QuQun ,Ghich)");
		stb.append(" VALUES");
		stb.append("(");
		for (int k = 0; k < j; k++) {
			stb.append("?, ");
		}
		stb.deleteCharAt(stb.length() - 2);
		stb.append(")");
//		String query = "INSERT INTO data VALUES(?, ?, ?, ?, ?,?,?,?)";
		System.out.println("QUERRY: " + stb.toString());
		PreparedStatement pre = connect.prepareStatement(stb.toString());

		while ((row = sheet.getRow(i)) != null) {
			try {
				j = 1;
//				cell = row.getCell(j);cell.getStringCellValue();
				while ((cell = row.getCell(j)) != null) {
					try {
						pre.setString(j, cell.getStringCellValue());
						System.out.println(cell.getStringCellValue());
					} catch (IllegalStateException e) {
						pre.setString(j, String.valueOf((int) cell.getNumericCellValue()));
					}
					j++;
				}
				pre.execute();
				i++;

			} catch (Exception e) {
				// TODO: handle exception
//				e.printStackTrace();
				System.out.println("SAI CẤU TRÚC");
			}

		}
		excel.close();
	}

	public void loadCSV(Connection connect, String csvFile, String tableName)
			throws ClassNotFoundException, SQLException, IOException {

		System.out.println("Connect DB Successfully :)");
		File f = new File(csvFile);

		if (!f.exists()) {
			System.out.println("File not exist!");
			return;
		}
		// Đọc File
		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = null;
//		String sql;
		// Đọc các field cuả file
		lineText = lineReader.readLine();
		// các field truyền vàp
		String fields[] = lineText.split(",");
		// tạo table với table name được truyền vào
//		sql = "CREATE table " + tableName + " (";
//		for (int i = 0; i < fields.length - 1; i++) {
//			sql += fields[i] + " CHAR(50),";
//		}
//		sql += fields[fields.length - 1] + " CHAR(50))";
//		System.out.println(sql);

//		PreparedStatement preparedStatement = connectionDB1.prepareStatement(sql);
//		preparedStatement.execute();
//		System.out.println("Create table Successfully :)");

		// insert dữ liệu vào table vừa tạo
		String query = "INSERT INTO " + tableName + " VALUES(";
		for (int i = 0; i < fields.length-1; i++) {
			query += "?,";
		}
		query += "?)";
		PreparedStatement pre = connect.prepareStatement(query);
		while ((lineText = lineReader.readLine()) != null) {

			try {
				String[] data = lineText.split(",");
				pre.setString(1, null);
				for (int i = 2; i < data.length +1 ; i++) {
					String d = data[i-1];
					pre.setString(i, d);
				}
//				System.out.println(pre.toString());
				pre.execute();
			} catch (Exception e) {
				System.out.println("SAI CẤU TRÚC");
			}

		}
		lineReader.close();
	}

	public ArrayList<ArrayList<String>> config() throws ClassNotFoundException, SQLException {
//		String s = "";
		String jdbcURL_1 = "jdbc:mysql://localhost/controldata?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "root";
		String password_1 = "";
		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);

		String sql = "Select * FROM myconfig";

		PreparedStatement p = connectionDB1.prepareStatement(sql);
		ResultSet resultSet = p.executeQuery(sql);
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();

		ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();

		while (resultSet.next()) {
			ArrayList<String> field = new ArrayList<String>();
			for (int i = 1; i <= columnsNumber; i++) {
//				if (i > 1)
//					System.out.print(",  ");
				String columnValue = resultSet.getString(i);
				field.add(columnValue);
//				System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
			}
			data.add(field);
//			System.out.println("");
		}

		return data;
	}

}
