import com.sun.mail.iap.ConnectionException;

import config.ProcessControlDB;
import extractData.DataStaging;
import myduyen.Download.DownloadFileServer;
import synologynas.exception.ListFileException;
import synologynas.exception.LoginException;

public class App {

	public static void main(String[] args) throws LoginException, ListFileException {
		final String idConfig = args[0];
		// process
		final ProcessControlDB p = new ProcessControlDB();
		// download file data
		final DownloadFileServer d = new DownloadFileServer();
		// load staging
		final DataStaging ex = new DataStaging();
		// load data warehouse
//		final LoadDataWareHouse loader = new LoadDataWareHouse();
		System.out.println("Start process");
		// begin 1 process
		int id = p.insert_updateProcess(-1, idConfig, "Start");
		// download
		try {
//			1. Hệ thống được lập lịch chạy 1phút/lần chạy theo từng id myconfig nhận vào:
//			insert process child => run(int idConfig)
			p.insert_updateChildProcess(id,idConfig, "StartDownload");
			d.run(idConfig);
//			12. Update status process child
			p.insert_updateChildProcess(id,idConfig, "EndDownload");
			System.out.println("end");
		}
		catch (ConnectionException e) {
			p.insert_updateChildProcess(id, idConfig, "ErrorDownload");
		}
		catch (Exception e) {
			p.insert_updateChildProcess(id, idConfig, "ErrorDownload");
		}
		

	// extract
	try

	{
		p.insert_updateChildProcess(id, "ETRACT", "Start");
		ex.run(idConfig);
		p.insert_updateChildProcess(id, "ETRACT", "End");

	}catch(
	Exception e)
	{
		p.insert_updateChildProcess(id, "ETRACT", "Error");

	}p.insert_updateProcess(id,idConfig,"End");

//					loader.connectDB();
//					loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien");

}}
