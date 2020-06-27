package myduyen.Download;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class SynologyNas {
	private String sid;
	private boolean isLoggedIn;
	private final String username;
	private final String password;
	private final String baseUrl;

	public SynologyNas(final String baseUrl, final String username, final String password) {
		this.baseUrl = baseUrl;
		this.username = username;
		this.password = password;
	}

	private Response getResponse(final String targetURL, final Map<String, String> params) {
		ResponseObjectHandler reponseHandler = new ResponseObjectHandler();
		HttpHelper.excuteGetWithHandler(targetURL, params, reponseHandler);
		return reponseHandler.responseObj;
	}

	private boolean getFile(final String targetURL, final Map<String, String> params, String filename) {
		FileDownloadResponseHandler fHandler = new FileDownloadResponseHandler(filename);
		HttpHelper.excuteGetWithHandler(targetURL, params, fHandler);
		return fHandler.isSuccess;
	}

	private ArrayList<String> retriveInfo(final String api) {
		final ArrayList<String> info = new ArrayList<String>();
		final String url = this.baseUrl + "/query.cgi";
		final LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
		params.put("api", "SYNO.API.Info");
		params.put("version", "1");
		params.put("method", "query");
		params.put("query", api);

		final Response response = getResponse(url, params);

		if (response.isSuccess() == false) {
			return null;
		}

		final JSONObject data = (JSONObject) response.getData().get(api);
		info.add("" + (Long) data.get("maxVersion"));
		info.add((String) data.get("path"));

		return info;
	}

	private boolean login() {
		final String url = this.baseUrl + "/auth.cgi";
		final LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
		params.put("api", "SYNO.API.Auth");
		params.put("version", "3");
		params.put("method", "login");

		params.put("account", username);
		params.put("passwd", password);
		params.put("session", "FileStation");
		params.put("format", "sid");

		final Response response = getResponse(url, params);

		if (response.isSuccess() == false) {
			System.out.println(response.getErrorCode());
			return false;
		}

		sid = (String) response.getData().get("sid");
		isLoggedIn = sid != null && !sid.isEmpty();
		return isLoggedIn;
	}

	public ArrayList<RemoteFile> list(final String folder, final int offset, final int limit) {
		return list(folder, offset, limit, "name", "asc");
	}

	public ArrayList<RemoteFile> list(final String folder, int offset, final int limit, final String sortBy,
			final String sortDirection) {
		if (!isLoggedIn) {
			if (!login()) {
				return null;
			}
		}

		final ArrayList<String> apiInfo = retriveInfo("SYNO.FileStation.List");

		final String url = this.baseUrl + "/" + apiInfo.get(1);
		final LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
		params.put("api", "SYNO.FileStation.List");
		params.put("_sid", sid);
		params.put("version", apiInfo.get(0));
		params.put("method", "list");

		params.put("folder_path", folder);
		params.put("offset", "" + offset);
		params.put("limit", "" + limit);
		params.put("sort_by", sortBy);
		params.put("sort_direction", sortDirection);
		params.put("additional", "time");

		final Response response = getResponse(url, params);

		if (response.isSuccess() == false) {
			System.out.println(response.getErrorCode());
			return null;
		}

		final JSONObject data = response.getData();
		final JSONArray files = (JSONArray) data.get("files");

		final ArrayList<RemoteFile> filePaths = new ArrayList<RemoteFile>();
		for (final Object file : files) {
			final String path = (String) ((JSONObject) file).get("path");
			final String name = (String) ((JSONObject) file).get("name");
			final boolean isDir = (Boolean) ((JSONObject) file).get("isdir");
			final JSONObject additional = (JSONObject) ((JSONObject) file).get("additional");

			final JSONObject time = (JSONObject) additional.get("time");
			final long mTime = (Long) time.get("mtime");
			filePaths.add(new RemoteFile(name, path, isDir, mTime));
		}

		return filePaths;
	}

	public String getMD5(String path) {
		if (!isLoggedIn) {
			if (!login()) {
				return null;
			}
		}

		final ArrayList<String> apiInfo = retriveInfo("SYNO.FileStation.List");

		final String url = this.baseUrl + "/" + apiInfo.get(1);
		final LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
		params.put("api", "SYNO.FileStation.MD5");
		params.put("_sid", sid);
		params.put("version", apiInfo.get(0));
		params.put("method", "start");
		params.put("file_path", path);
		Response response = getResponse(url, params);

		if (response.isSuccess() == false) {
			System.out.println(response.getErrorCode());
			return null;
		}

		JSONObject data = response.getData();
		final String taskid = "\"" + (String) data.get("taskid") + "\"";
		// set taskid
		String md5;
		while (true) {
			params.clear();
			params.put("api", "SYNO.FileStation.MD5");
			params.put("_sid", sid);
			params.put("version", apiInfo.get(0));
			params.put("method", "status");
			params.put("taskid", taskid);
			response = getResponse(url, params);

			if (response.isSuccess() == false) {
				System.out.println(response.getErrorCode());
				return null;
			}

			data = response.getData();

			final boolean finished = (Boolean) data.get("finished");
			if (finished) {
				md5 = (String) data.get("md5");
				break;
			}

		}
//		// stop
//		params.clear();
//		params.put("api", "SYNO.FileStation.MD5");
//		params.put("_sid", sid);
//		params.put("version", apiInfo.get(0));
//		params.put("method", "stop");
//		params.put("taskid", taskid);
//		response = getResponse(url, params);
//
//		if (response.isSuccess() == false) {
//			System.out.println(response.getErrorCode());
//			return null;
//		}

		return md5;
	}

	public String getDigest(String pathFile, int byteArraySize) throws NoSuchAlgorithmException, IOException {

		MessageDigest md = MessageDigest.getInstance("MD5");
		md.reset();
		InputStream is = new FileInputStream(pathFile);

		byte[] bytes = new byte[byteArraySize];
		int numBytes;
		while ((numBytes = is.read(bytes)) != -1) {
			md.update(bytes, 0, numBytes);
		}
		is.close();
		byte[] digest = md.digest();
		String result = new String(Hex.encodeHex(digest));
		return result;
	}

	public boolean download(String remoteFile, String localFile) {
		if (!isLoggedIn) {
			if (!login()) {
				return false;
			}
		}

		final String api = "SYNO.FileStation.Download";
		final ArrayList<String> apiInfo = retriveInfo(api);
		final String url = this.baseUrl + "/" + apiInfo.get(1);
		final LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
		params.put("api", api);
		params.put("version", apiInfo.get(0));
		params.put("method", "download");
		params.put("_sid", sid);

		params.put("path", remoteFile);
		params.put("mode", "download");

		return getFile(url, params, localFile);
	}
}

class FileDownloadResponseHandler implements ResponseHandler {
	private String filname;
	public boolean isSuccess;

	public FileDownloadResponseHandler(String filname) {
		this.filname = filname;
	}

	public void handle(InputStream is) {
		try {
			byte[] buffer = new byte[1024];
			File targetFile = new File(filname);
			OutputStream outStream = new FileOutputStream(targetFile);
			int size = 0;
			while ((size = is.read(buffer)) != -1) {
				outStream.write(buffer, 0, size);
				outStream.flush();
			}

			isSuccess = true;
			outStream.close();
		} catch (IOException e) {
			e.printStackTrace();
			isSuccess = false;
		}
	}
}