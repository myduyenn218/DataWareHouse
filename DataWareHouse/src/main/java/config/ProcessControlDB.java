package config;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import dao.Process;

public class ProcessControlDB {

	public void insert(String idConfig) {

		Connection connection;
		int id = Integer.parseInt(idConfig);
		String sql = "INSERT INTO process SET process_config_id=?";
		try {
			connection = OpenConnection.openConnectWithDBName("controldata");
			PreparedStatement ps1 = connection.prepareStatement(sql);
			ps1.setInt(1, id);
			ps1.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void update(String idConfig) {

		Connection connection;
		int id = Integer.parseInt(idConfig);
		String sql = "INSERT INTO process SET idConfig=?, status=?";
		try {
			connection = OpenConnection.openConnectWithDBName("controldata");
			PreparedStatement ps1 = connection.prepareStatement(sql);
			ps1.setInt(1, id);
			ps1.setString(2, "Start");
			ps1.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
