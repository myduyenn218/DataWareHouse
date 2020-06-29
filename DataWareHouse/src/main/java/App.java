import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ExtractData.ExtractDataApp;
import load_datawarehouse.LoadDataWareHouse;
import myduyen.Download.DownloadFileServer;

public class App {

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		// download file data
		final DownloadFileServer d = new DownloadFileServer();
		// load staging
		final ExtractDataApp ex = new ExtractDataApp();
		// load data warehouse
		final LoadDataWareHouse loader = new LoadDataWareHouse();
		System.out.println("aaaaa");
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		ses.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					d.run();
					ex.startExtract();
					loader.load();
					loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien", "");
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 0, 1, TimeUnit.MINUTES); // 1p 1 lần

	}
}