package synologynas;

public class RemoteFile {
	private String path;
	private boolean isDir;
	private String name;
	private long modifyTime;

	public boolean isDir() {
		return isDir;
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}
	public String getTypeFile() {
		String typeFile = name.substring(name.indexOf('.')+ 1);
		return typeFile;
	}

	public long getModifyTime() {
		return modifyTime;
	}

	public RemoteFile(String name, String path, boolean isDir, long modifyTime) {
		this.path = path;
		this.isDir = isDir;
		this.name = name;
		this.modifyTime = modifyTime;
	}

}