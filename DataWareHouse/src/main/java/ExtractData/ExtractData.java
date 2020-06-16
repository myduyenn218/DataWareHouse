package ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
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

public class ExtractData {

	private Connection connectionDB1;
	private Connection connectionDB2;

	public ExtractData(Connection connectionDB1, Connection connectionDB2) {
		super();
		this.connectionDB1 = connectionDB1;
		this.connectionDB2 = connectionDB2;
	}

	public ExtractData() {
		// TODO Auto-generated constructor stub
	}

	public void readExcel(String des_db, String user_sr, String password, String excelFile, String tableName)
			throws ClassNotFoundException, SQLException, IOException {
		Connection connect = DBConnection.getConnection(des_db, user_sr, password);
		System.out.println("Connect DB Successfully 🙂");
		Workbook excel = new XSSFWorkbook(excelFile);
		Sheet sheet = excel.getSheetAt(0);
		Row row;
		Cell cell;
		int i = 1, j = 0;
//		String sql = "CREATE TABLE "
//				+ "data(mssv VARCHAR(100),class VARCHAR(300),"
//				+ "department VARCHAR(255),faculty VARCHAR(255),"
//				+ "gender VARCHAR(50), fullname VARCHAR(255),"
//				+ "palceofbirth VARCHAR(255),schoolyear VARCHAR(255))";
		StringBuffer stb = new StringBuffer("CREATE TABLE ");
		stb.append(tableName);
		stb.append("(");
		row = sheet.getRow(0);
		while ((cell = row.getCell(j)) != null) {

			stb.append(cell.getStringCellValue().replaceAll("[^a-zA-Z0-9]", ""));
			stb.append(" VARCHAR(255),");
			j++;
		}
		stb.deleteCharAt(stb.length() - 1);
		stb.append(")");
		System.out.println(stb.toString());
		PreparedStatement preparedStatement = connect.prepareStatement(stb.toString());
		preparedStatement.execute();
		stb = new StringBuffer("INSERT INTO ");
		stb.append(tableName);
		stb.append(" VALUES");
		stb.append("(");
		for (int k = 0; k < j; k++) {
			stb.append("?, ");
		}
		stb.deleteCharAt(stb.length() - 2);
		stb.append(")");
//		String query = "INSERT INTO data VALUES(?, ?, ?, ?, ?,?,?,?)";
		System.out.println(stb.toString());
		PreparedStatement pre = connect.prepareStatement(stb.toString());

		while ((row = sheet.getRow(i)) != null) {
			j = 0;
			while ((cell = row.getCell(j)) != null) {
				try {
					pre.setString(j + 1, cell.getStringCellValue());
					System.out.println(cell.getStringCellValue());
				} catch (IllegalStateException e) {
					pre.setString(j + 1, String.valueOf((int) cell.getNumericCellValue()));
				}
				j++;
			}
			pre.execute();
			i++;

		}
		excel.close();
	}

	public void load(String delimited, String pathFile, String fieldName, String tableName)
			throws ClassNotFoundException, SQLException, IOException {

		System.out.println("Connect DB Successfully :)");
		File f = new File(pathFile);

		if (!f.exists()) {
			System.out.println("File not exist!");
			return;
		}
		// Đọc File
		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = null;
		String sql;
		// Đọc các field cuả file
		lineText = lineReader.readLine();
		// tác các field truyền vàp
		String fields[] = fieldName.split(delimited);
		// tạo table với table name được truyền vào
		sql = "CREATE table " + tableName + " (";
		for (int i = 0; i < fields.length - 1; i++) {
			sql += fields[i] + " CHAR(50),";
		}
		sql += fields[fields.length - 1] + " CHAR(50))";
		System.out.println(sql);

		PreparedStatement preparedStatement = connectionDB1.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// insert dữ liệu vào table vừa tạo
		String query = "INSERT INTO " + tableName + " VALUES(";
		for (int i = 0; i < fields.length - 1; i++) {
			query += "?,";
		}
		query += "?)";

		PreparedStatement pre = connectionDB1.prepareStatement(query);
		while ((lineText = lineReader.readLine()) != null) {
			String[] data = lineText.split(delimited);
			for (int i = 0; i < data.length; i++) {
				String d = data[i];
				pre.setString(i + 1, d);
			}
			pre.execute();
		}
		lineReader.close();
	}

	public void copy(String database1, String nameTableDB1, String database2, String nameTableDB2, String fieldName)
			throws ClassNotFoundException, SQLException {

		ResultSet rs;
		Statement stmt = connectionDB1.createStatement();
		String sqlSelectNameColumn = "SELECT * FROM " + nameTableDB1;
		System.out.println(sqlSelectNameColumn);
		rs = stmt.executeQuery(sqlSelectNameColumn);
		ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
		int counter = md.getColumnCount();
		String colName[] = new String[counter];
		System.out.println("The column names are as follows:");
		for (int loop = 1; loop <= counter; loop++) {
			colName[loop - 1] = md.getColumnLabel(loop);
//			sqlCreateTable += colName[loop - 1] + " CHAR(50),";
		}
		String sql = "";
		String fields[] = fieldName.split(",");
		// tạo table với table name được truyền vào
		sql = "CREATE table " + nameTableDB2 + " (";
		for (int i = 0; i < fields.length - 1; i++) {
			sql += fields[0] + " CHAR(50),";
		}
		sql += fields[0] + " CHAR(50))";
		System.out.println(sql);

		String sqlCreateTable = "CREATE table " + nameTableDB2 + "(" + colName[0] + " VARCHAR(15)," + colName[1]
				+ " CHAR(50)," + colName[2] + " CHAR(50)," + colName[3] + " CHAR(50)," + colName[4] + " CHAR(50))";
//		sqlCreateTable += ")";

		System.out.println(sqlCreateTable);
		PreparedStatement p = connectionDB2.prepareStatement(sqlCreateTable);
		p.execute();

//		COPY 
		String insert = "INSERT INTO " + database2 + "." + nameTableDB2 + " SELECT * FROM " + database1 + "."
				+ nameTableDB1;
		System.out.println(insert);
		PreparedStatement pc = connectionDB2.prepareStatement(insert);
		pc.execute();
	}

	public ArrayList<ArrayList<String>> config() throws ClassNotFoundException, SQLException {
//		String s = "";
		String jdbcURL_1 = "jdbc:mysql://localhost/controldata?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String userName_1 = "admin";
		String password_1 = "admin";
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
