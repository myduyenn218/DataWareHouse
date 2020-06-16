package myduyen.Download;

public class RemoteFile {
	private String path;
	private boolean isDir;
	private String name;

	public boolean isDir() {
		return isDir;
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public RemoteFile(String name, String path, boolean isDir) {
		this.path = path;
		this.isDir = isDir;
		this.name = name;
	}
}