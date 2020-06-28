package config;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadProperties {
	private static final String FILE_CONFIG = "/config.properties";

	public static String getProperty(String key) {

		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			String currentDir = System.getProperty("user.dir");
			inputStream = new FileInputStream(currentDir + FILE_CONFIG);

			// load properties from file
			properties.load(inputStream);

			// get property by name
			return properties.getProperty(key);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// close objects
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;

	}

	public static void main(String[] args) {

	}
}
