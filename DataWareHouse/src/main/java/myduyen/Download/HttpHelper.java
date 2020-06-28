package myduyen.Download;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

interface ResponseHandler {
  void handle(InputStream is);
}

class HttpHelper {
	// mã hóa params truyền vào dạng url encode
  private static String encodeParams(final Map<String, String> params) throws UnsupportedEncodingException {
    final StringBuilder result = new StringBuilder();

    for (final Map.Entry<String, String> entry : params.entrySet()) {
      result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
      result.append("=");
      result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      result.append("&");
    }

    final String resultString = result.toString();
    // kiểm tra và xóa dấu & cuối cùng
    return resultString.length() > 0 ? resultString.substring(0, resultString.length() - 1) : resultString;
  }

  // gửi get request và nhận vào các tham số truyền vào (API)
  
  public static void excuteGetWithHandler(final String targetURL, final Map<String, String> params,
      final ResponseHandler handler) {
    URL url;
    HttpURLConnection connection = null;
    try {
      final String urlParameters = encodeParams(params);
      url = new URL(targetURL + "?" + urlParameters);
      System.out.println("GET " + url);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      // Get Response
      final InputStream is = connection.getInputStream();
      // hàm để xử lý inputstream 
      handler.handle(is);
      is.close();
    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}