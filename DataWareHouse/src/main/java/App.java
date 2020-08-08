import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import config.ProcessControlDB;
import extractData.DataStaging;
import myduyen.Download.DownloadFileServer;

public class App {

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, NoSuchAlgorithmException, IOException {
		final String idConfig = args[0];
		// process
		final ProcessControlDB p = new ProcessControlDB();
		// download file data
		final DownloadFileServer d = new DownloadFileServer();
		// load staging
		final DataStaging ex = new DataStaging();
		// load data warehouse
//		final LoadDataWareHouse loader = new LoadDataWareHouse();
		System.out.println("start");
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		ses.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				//begin 1 process
				p.insert(idConfig);
				// download
//					d.run(idConfig);
				System.out.println("end");
				
				// extract
				ex.run(idConfig);
//					loader.connectDB();
//					loader.copy("warehousedata", "sinhvien", "warehousedata", "sinhvien");
			}
		}, 0, 1, TimeUnit.MINUTES); // 5p 1 lần

	}
}
