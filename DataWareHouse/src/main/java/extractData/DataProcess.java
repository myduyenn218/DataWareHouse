package extractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import config.OpenConnection;
import dao.ControlDB;

public class DataProcess {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static final String DATE_FORMAT = "yyyy-MM-dd";
	static final String ACTIVE_DATE = "31-12-2013";
	private ControlDB cdb;
	private String config_db_name;
	private String target_db_name;
	private String table_name;

	public DataProcess() {
		cdb = new ControlDB(this.config_db_name, this.table_name, this.target_db_name);
	}

	// Phương thức đọc những giá trị có trong file (value)
	// cách nhau bởi dấu phân cách (delim).
	private String readLines(String value, String delim) {
		// dữ liệu là 1 chuỗi
		String values = "";
		// cắt thành từng stoken dựa theo delim
		StringTokenizer stoken = new StringTokenizer(value, delim);
		// if (stoken.countTokens() > 0) {
		// stoken.nextToken();
		// }
		// số token sẽ bằng số trường
		int countToken = stoken.countTokens();
		// đọc từng dòng mở đầu bằng dấu "("
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
			// nếu kiểu dữ liệu là số thì sẽ bt  dạng (17130008,....)
			if (Pattern.matches(NUMBER_REGEX, token)) {
				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
			} else {
				// nếu kiểu dữ liệu không là số thì bỏ vào dấu nháy đơn dạng (17130008,'abc',...)
				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
			}
			// dữ liệu trả về là chuỗi các dòng
			values += lines;
			lines = "";
		}
		return values;
	}

	// phương thức đọc file txt
	public String readValuesTXT(File s_file, int count_field) {
		// Nếu không tồn tại file thì trả về null
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim = "|"; // hoặc \t
		try {
			// Đọc 1 dòng dữ liệu trong file
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8"));
			String line = bReader.readLine();
			// dữ liệu được cắt bởi dấu "\t"
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			// Kiểm tra xem tổng số field trong file có đúng format hay không
			// có 11 trường
			if (new StringTokenizer(line, delim).countTokens() != (count_field + 1)) {
				bReader.close();
				return null;
			}
			// STT|Mã sinh viên|Họ lót|Tên|...-> line.split(delim)[0]="STT"
			// không phải số
			// nên là header -> bỏ qua line
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) { // Kiem tra xem có phần header khong
				// sau đó sẽ lấy dữ liệu từng dòng
				values += readLines(line + delim, delim);
			}
			while ((line = bReader.readLine()) != null) {
				// line = 1|17130005|Đào Thị Kim|Anh|15-08-1999|DH17DTB|
				// Công nghệ thông tin b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc
				// line + " " + delim = 1|17130005|Đào Thị Kim Anh|15-08-1999|DH17DTB|
				// Công nghệ thông tin b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc |
				// Nếu có field 11 thì dư khoảng trắng lên readLines() có
				// trim(), còn 10 field
				// thì fix lỗi out index
				values += readLines(line + " " + delim, delim);
			}
			bReader.close();
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	// Lay tat ca cac truong co trong staging:
	public static ResultSet selectAllField(String db_name, String table_name) {
		ResultSet rs = null;
		Connection conn = null;
		try {
			// kết nối datastaging
			conn = OpenConnection.openConnectWithDBName(db_name);
			// selcet tất cả trường của staging
			String selectConfig = "select * from " + table_name;
			// thực hiện câu select
			PreparedStatement ps = conn.prepareStatement(selectConfig);
			// trả về resultset
			return rs = ps.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				// kết quả rỗng thì kết thúc
				if (rs != null)
					rs.close();
				// Không kết nối được cũng kết thúc
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	// Phương thức đọc dữ liệu trong file .xlsx:
	public String readValuesXLSX(File s_file, int countField) {
		String values = "";
		String value = "";
		String delim = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn);
			// lấy từng sheet
			XSSFSheet sheet = workBook.getSheetAt(0);
			// lấy từng hàng trong sheet
			Iterator<Row> rows = sheet.iterator();
			// Kiểm tra xem có phần header hay không, nếu không có phần header
			// Gọi rows.next, nếu có header thì vị trí dòng dữ liệu là 1.
			// Nếu kiểm tra mà không có header thì phải set lại cái row bắt đầu ở vị trí 0
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				rows = sheet.iterator();
			}
			while (rows.hasNext()) {
				Row row = rows.next();
				// Kiểm tra coi cái số trường ở trong file excel có đúng với
				// số trường có trong cái bảng mình tạo sẵn ở trong table
				// staging không
//				if (row.getLastCellNum() < countField - 1 || row.getLastCellNum() > countField) {
//					workBook.close();
//					return null;
//				}
				// Bắt đầu lấy giá trị trong các ô ra:
				// Iterator<Cell> cells = row.cellIterator();
				for (int i = 0; i < countField; i++) {
					// Cell cell = cells.next();
					if (i == countField - 1) {
						value += DataProcess.ACTIVE_DATE;
					}
					Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim;
						} else {
							value += (long) cell.getNumericCellValue() + delim;
						}
						break;
					case STRING:
						value += cell.getStringCellValue() + delim;
						break;
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim;
							break;
						default:
							value += " " + delim;
							break;
						}
						break;
					case BLANK:
					default:
						// kiểm tra lỗi file những dòng cuối cùng của file 
						// 2 cột đầu cho dạng int 
						if (i < 2) {
							value += (long) cell.getNumericCellValue() + delim;
						} else
							//còn lại là dạng chuỗi
							value += " " + delim;
						break;
					}
				}
				if (row.getLastCellNum() == countField) {
					value += "|";
				}
				values += readLines(value, delim);
				value = "";
			}
			workBook.close();
			fileIn.close();
			return values.substring(0, values.length() - 1);
		} catch (Exception e) {
			return null;
		}
	}

	// Ghi dữ liệu vào data staging
	public boolean writeDataToBD(String column_list, String target_table, String values) throws ClassNotFoundException {
		try {
			if (cdb.insertValues(column_list, values, target_table))
				return true;
		} catch (ClassNotFoundException | NoSuchAlgorithmException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void setConfig_db_name(String config_db_name) {
		this.config_db_name = config_db_name;
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

	public ControlDB getCdb() {
		return cdb;
	}

	public void setCdb(ControlDB cdb) {
		this.cdb = cdb;
	}

}
