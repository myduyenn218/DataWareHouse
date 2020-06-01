import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ExtractData {

	private Connection connectionDB1;
	private Connection connectionDB2;

	public ExtractData(Connection connectionDB1, Connection connectionDB2) {
		super();
		this.connectionDB1 = connectionDB1;
		this.connectionDB2 = connectionDB2;
	}

	public void load(String delimited, String pathFile) throws ClassNotFoundException, SQLException, IOException {

		System.out.println("Connect DB Successfully :)");
		File f = new File(pathFile);

		if (!f.exists()) {
			System.out.println("File not exist!");
			return;
		}

		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = null;

		int count = 0;
		String sql;

		lineText = lineReader.readLine();
		String[] fields = lineText.split(delimited);
		System.out.println(fields.length);
		System.out.println(lineText);
		// create table
		
		String nameFile = f.getName();
		String nameTable[] = nameFile.split("\\.");
		System.out.println(nameFile);
		
		sql = "CREATE table " + nameTable[0] + " (" + fields[0] + " CHAR(50)," + fields[1] + " CHAR(50)," + fields[2]
				+ " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connectionDB1.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// skip header line
		String query = "INSERT INTO " + nameTable[0] + " VALUES(?, ?, ?, ?, ?)";
		PreparedStatement pre = connectionDB1.prepareStatement(query);
		while ((lineText = lineReader.readLine()) != null) {
			String[] data = lineText.split(",");
			String id = data[0];
			String name = data[1];
			String gender = data[2];
			String dateOfBirth = data[3];
			String phone = data[4];
			pre.setString(1, id);
			pre.setString(2, name);
			pre.setString(3, gender);
			pre.setString(4, dateOfBirth);
			pre.setString(5, phone);
			pre.execute();
		}
	}

	public void copy(String database1, String nameTableDB1, String database2, String nameTableDB2)
			throws ClassNotFoundException, SQLException {

		ResultSet rs;
		Statement stmt = connectionDB1.createStatement();
		String sqlSelectNameColumn = "SELECT * FROM " + nameTableDB1;
		rs = stmt.executeQuery(sqlSelectNameColumn);
		ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
		int counter = md.getColumnCount();
		String colName[] = new String[counter];
		System.out.println("The column names are as follows:");
		for (int loop = 1; loop <= counter; loop++) {
			colName[loop - 1] = md.getColumnLabel(loop);
//			sqlCreateTable += colName[loop - 1] + " CHAR(50),";
		}
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
}
