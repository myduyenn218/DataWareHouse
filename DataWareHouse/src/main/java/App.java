import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.ecepvn.date_dim.Date_Dim;

import config.DBConnection;
import config.OpenConnection;
import load_datawarehouse.LoadDataWareHouse;
import myduyen.Download.DownloadFileServer;

public class App {

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		final String idConfig = args[0];
		// download file data
		final DownloadFileServer d = new DownloadFileServer();
		// load staging
//		final ExtractDataApp ex = new ExtractDataApp();
		// load data warehouse
		final LoadDataWareHouse loader = new LoadDataWareHouse();
		System.out.println("start");
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		ses.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					d.run(idConfig);
//					ex.startExtract();
//					loader.connectDB();
//					loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien");
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
		}, 0, 5, TimeUnit.MINUTES); // 1p 1 lần

	}
}
