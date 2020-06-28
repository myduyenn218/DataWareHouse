package myduyen.Download;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

class ResponseObjectHandler implements ResponseHandler {
    Response responseObj;

    public void handle(InputStream is) {
        String responseStr;

        final BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        String line;
        final StringBuffer response = new StringBuffer();

        try {
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }

        responseStr = response.toString();
        System.out.println("RESPONSE: " + responseStr);
        responseObj = new Response(responseStr);
    }

}
// response trên api

class Response {
    private boolean isSuccess;
    private JSONObject data;
    private JSONObject error;

    Response(final String responseStr) {
        final JSONParser parser = new JSONParser();

        try {
            final JSONObject obj = (JSONObject) parser.parse(responseStr);
            isSuccess = (Boolean) obj.get("success");
            if (isSuccess) {
                data = (JSONObject) obj.get("data");
            } else {
                error = (JSONObject) obj.get("error");
            }
        } catch (final ParseException pe) {
            isSuccess = false;
            error = new JSONObject();
        }
    }

    boolean isSuccess() {
        return isSuccess;
    }

    JSONObject getData() {
        return data;
    }

    long getErrorCode() {
        try {
            return (Long) error.get("code");
        } catch (Exception e) {
            return 0;
        }
    }

    JSONArray getErrorDetail() {
        try {
            return (JSONArray) error.get("errors");
        } catch (Exception e) {
            return null;
        }
    }
}