package extractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
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

import config.OpenConnection;
import config.SendMail;
import dao.Config;
import dao.ControlDB;
import dao.Log;

public class DataStaging {
	SendMail sendmail = null;

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
//	public void truncateData() {
//		
//		
//	}
	
	public DataStaging() {
		this.sendmail = sendmail;
	}


	// khai báo để gọi sang chạy hệ thống
public void run(String idCongfig) {
		
		//  set id từ bảng myconfig với id là idConfig nhập vào
		//  set status là state = "OK"
		ControlDB cdb = new ControlDB();
		cdb.setConfig_db_name("controldata");
		cdb.setTarget_db_name("stagingdata");
		
		cdb.setTable_name("myconfig");
		int id = Integer.parseInt(idCongfig);
		setConfig_id(id);
		setState("OK");

		// Gọi lại lớp controlDB từ lớp DataProcess
		DataProcess dp = new DataProcess();
		dp.setCdb(cdb);
		
		try {
			// bắt đầu chạy extracdata
			extractToDB(dp);
		} catch (ClassNotFoundException | NoSuchAlgorithmException | SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void extractToDB(DataProcess dp)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		// 1.KẾT NỐI VỚI controldata ở ControlDB
		
		// 2. LẤY CÁC DỮ LIỆU TỪ LOGS VÀ CONFIG 
		// list các config
		List<Config> lstConf = dp.getCdb().loadAllConfs(this.config_id);
		// Lấy các trường trong config
		for (Config configuration : lstConf) {
			String extention = "";
			String target_table = configuration.getTable_name_staging();
			String import_dir = configuration.getPath_file_local();
			String delim = ",";
			String column_list = configuration.getFields();

			// Lấy các các dòng ứng với status ứng với từng config;
			Log log = dp.getCdb().getLogsWithStatus(this.state, this.config_id);

			// Lấy config_name từ trong config ra
			String file_name = log.getIdLog();
			// Đường dẫn dẫn tới file cần load
			String sourceFile = import_dir + file_name;
			// Đếm số trường trong fields trong bảng config
			StringTokenizer str = new StringTokenizer(column_list, delim);
			
			System.out.println(sourceFile);
			// 2.1. FILE LẤY ĐƯỢC TỪ LOGS VÀ CONFIG
			File file = new File(sourceFile);
			// System.out.println(file.exists());
			// Lấy đuôi file ra xem đó là kiểu file gì để xử lí đọc file
			extention = file.getName().substring(file.getName().indexOf('.'));
			// truncate data exist

			// 3. KIỂM TRA FILE CÓ TỒN TẠI HAY KHÔNG
			if (file.exists()) {
				
				// lấy file có status OK
				System.out.println("status: " + log.getStatus());
				if (log.getStatus().equals("OK")) {
					String transform;
					String status;
					// set thời gian
					String timestamp = getCurrentTime();
					String values = "";
					
					// 4. ĐỌC FILE TXT, XLSX 
					// Nếu file là .txt thì đọc file .txt
					if (extention.equals(".txt")) {
						// đọc file txt với số trường là số token được
						values = dp.readValuesTXT(file, str.countTokens());
						extention = ".txt";
						// Nếu file là .xlsx thì đọc file .xlsx
					} else if (extention.equals(".xlsx")) {
						// đọc file xlsx với số trường là số token được
						values = dp.readValuesXLSX(file, str.countTokens());
						extention = ".xlsx";
					}
//					
					
					// 5. KIỂM TRA DỮ LIỆU NULL
					if (values != null) {
//						String table = "logs";
						int config_id = configuration.getIdConf();
						// 6. KẾT NỐI DB STAGING
						Connection conn = OpenConnection.openConnectWithDBName("stagingdata");
						
						// 7. KIỂM TRA BẢNG TỒN TẠI
						if (dp.getCdb().checkTableExist(conn, target_table, "stagingdata") == 0) {
							// 7.1. Thông báo chưa có bảng
							System.out.println("Bảng không tồn tại");
							sendmail.sendMail("Load thất bại", "Bảng không tồn tại", "");
//							status = "Not TR";
//							transform = "FAIL";
//							dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);

						} else {
							//8. THỰC HIỆN GHI DỮ LIỆU VÀO STAGING 
							if (dp.writeDataToBD(column_list, target_table, values)) {
								// nếu ghi được rồi
								//  UPDATE LOGS
								//   updateLog transform = NotReadyTransfrom thành OK, status = OK thành TR
								status = "TR";
								transform = "OK";
								dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
//								target_dir = configuration.getSuccessDir();
//								 if (moveFile(target_dir, file));

								// Nếu không ghi được
							} else {
								// thì updateLog transform = NotReadyTransfrom thành FAIL, status = OK thành Not TR
								status = "Not TR";
								transform = "FAIL";
								dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
								sendmail.sendMail("Load thất bại", "", "");
							
							}
						}

					} else {
						// Xử lí khi dữ liệu bị NULL
						status = "Not TR";
						transform = "FAIL";
						dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
						sendmail.sendMail("Load thất bại", "Dữ liệu bị NULL", "");
					
					}
				}

			} else {
				// 3.1. THÔNG BÁO FILE KHÔNG TỒN TẠI
				String transform;
				String status;
				String timestamp = getCurrentTime();
			
				// Không còn file hoặc k tìm được file sẽ sysout
				System.out.println("Path not exists!!!");
				// UPDATE LOGS
				status = "Not TR";
				transform = "FAIL";
				dp.getCdb().updateLogAfterLoadToStaging(status, transform, timestamp, file_name);
				sendmail.sendMail("Load thất bại", "Không tìm được file", "");
	return;
			}

		}

	}

	// Phương thức lấy ra thời gian hiện tạo để ghi vào log :
	public String getCurrentTime() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		return dtf.format(now);
	}

}
