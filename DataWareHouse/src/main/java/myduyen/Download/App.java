package myduyen.Download;

import java.util.ArrayList;

/**
 * App
 */
public class App {
	public static void main(String[] args) {
		SynologyNas nas = new SynologyNas("http://drive.ecepvn.org:5000/webapi", "guest_access", "123456");

		ArrayList<RemoteFile> filePaths = nas.list("/ECEP/song.nguyen/DW_2020/data", 0, 0);
		for (RemoteFile file : filePaths) {
			System.out.printf("Name: %s, Path: %s, isDir: %s\n", file.getName(), file.getPath(), file.isDir());
		}
		nas.download("/ECEP/song.nguyen/DW_2020/data", "/home/myduyen/data.zip");
	}
}