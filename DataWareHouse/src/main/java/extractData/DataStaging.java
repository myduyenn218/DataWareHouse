package extractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.Config;
import dao.ControlDB;
import dao.Log;

public class DataStaging {
	static final String EXT_TEXT = ".txt";
	static final String EXT_CSV = ".csv";
	static final String EXT_EXCEL = ".xlsx";
	private int config_id;
	private String state;

	public String getState() {
		return state;
	}

	public int getConfig_id() {
		return config_id;
	}

	public void setConfig_id(int config_id) {
		this.config_id = config_id;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void run(String idCongfig) {
		int id = Integer.parseInt(idCongfig);
		setConfig_id(id);
		setState("OK");
		DataProcess dp = new DataProcess();
		ControlDB cdb = new ControlDB();
		cdb.setConfig_db_name("controldata");
		cdb.setTarget_db_name("stagingdata");
		cdb.setTable_name("myconfig");
		dp.setCdb(cdb);
		try {
			extractToDB(dp);
		} catch (ClassNotFoundException | NoSuchAlgorithmException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void extractToDB(DataProcess dp)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		List<Config> lstConf = dp.getCdb().loadAllConfs(this.config_id);
		// Lấy các trường trong config
		for (Config configuration : lstConf) {
			String extention = "";
			String target_table = configuration.getTable_name_staging();
			String import_dir = configuration.getPath_file_local();
			String delim = ",";
			String column_list = configuration.getFields();
//			String variabless = configuration.getVariabless();
			System.out.println(target_table);
			// Lấy các trường có trong dòng log đầu tiên có status = OK;
			Log log = dp.getCdb().getLogsWithStatus(this.state, this.config_id);

			// Lấy config_name từ trong config ra
			String file_name = log.getIdLog();
			// Đường dẫn dẫn tới file cần load
			String sourceFile = import_dir + file_name;
			// Đếm số trường trong fields trong bảng config
			StringTokenizer str = new StringTokenizer(column_list, delim);
			System.out.println(sourceFile);
			File file = new File(sourceFile);
			// System.out.println(file.exists());
			// Lấy đuôi file ra xem đó là kiểu file gì để xử lí đọc file
			extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL
					: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
			if (file.exists()) {
				if (log.getStatus().equals("OK")) {
					String values = "";
					// Nếu file là .txt thì đọc file .txt
					if (extention.equals(".txt")) {
						values = dp.readValuesTXT(file, str.countTokens());
						extention = ".txt";
						// Nếu file là .xlsx thì đọc file .xlsx
					} else if (extention.equals(".xlsx")) {
						values = dp.readValuesXLSX(file, str.countTokens());
						extention = ".xlsx";
					}
					System.out.println(values);
					// Nếu mà đọc đc dữ liệu rồi
					if (values != null) {
						String table = "logs";
						String transform;
						String status;
						int config_id = configuration.getIdConf();
						// set thời gian
						String timestamp = getCurrentTime();
						// đếm số dòng
						String stagin_load_count = "";
						try {
							stagin_load_count = countLines(file, extention) + "";
						} catch (InvalidFormatException
								| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
							e.printStackTrace();
						}
						//
						String target_dir;
						// rồi ghi dữ liệu vô bảng
						// nếu ghi được rồi

						if (dp.writeDataToBD(column_list, target_table, values)) {
							// thì updateLog transform = NotReadyTransfrom thành OK, status = OK thành TR
							status = "TR";
							transform = "OK";
							dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
//							target_dir = configuration.getSuccessDir();
//							 if (moveFile(target_dir, file));
							// Nếu không ghi được
						} else {
							// thì updateLog transform = NotReadyTransfrom thành FAIL, status = OK thành Not
							// TR
							status = "Not TR";
							transform = "FAIL";
							dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
//							target_dir = configuration.getErrorDir();
//							if (moveFile(target_dir, file));
						}
					}
				}

			} else {
				// Không còn file hoặc k tìm được file sẽ sysout
				System.out.println("Path not exists!!!");
				return;
			}

		}

	}

	// Phương thức lấy ra thời gian hiện tạo để ghi vào log đeer:
	public String getCurrentTime() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		return dtf.format(now);
	}

	// Dem so dong cua file do:
	// còn lỗi
	private int countLines(File file, String extention)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(".txt") != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line;
				while ((line = bReader.readLine()) != null) {
					if (!line.trim().isEmpty()) {
						result++;
					}
				}
				bReader.close();
			} else if (extention.indexOf(".xlsx") != -1) {
				workBooks = new XSSFWorkbook(file);
				XSSFSheet sheet = workBooks.getSheetAt(0);
				Iterator<Row> rows = sheet.iterator();
				rows.next();
				while (rows.hasNext()) {
					rows.next();
					result++;
				}
				return result;
			}

		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
			e.printStackTrace();
		} finally {
			if (workBooks != null) {
				try {
					workBooks.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
}
