package config;

import java.beans.Statement;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.cj.protocol.Resultset;

import dao.Process;

public class ProcessControlDB {

	public int insert_updateChildProcess(int process_id, String name, String status) {

		Connection connection;

		String sql = "INSERT INTO child_process (process_id, name, status ) VALUES (?,?,?) ON DUPLICATE KEY UPDATE status = ?";
		try {
			connection = OpenConnection.openConnectWithDBName("controldata");
			PreparedStatement ps1 = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
			ps1.setInt(1, process_id);
			ps1.setString(2, name);
			ps1.setString(3, status);
			ps1.setString(4, status);
			ps1.execute();
			ResultSet re = ps1.getGeneratedKeys();
			if (re.next()) {
				return re.getInt(1);
			}
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
		return -1;
	}

	public int insert_updateProcess(int processId, String stIdConfig, String status) {

		Connection connection;
		int intIdConfig = Integer.parseInt(stIdConfig);

		String sql = "INSERT INTO process (process_id, idConfig, status) VALUES (?,?,?) ON DUPLICATE KEY UPDATE  status=?";
		try {
			connection = OpenConnection.openConnectWithDBName("controldata");
			PreparedStatement ps1 = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
			if (processId == -1) {
				ps1.setNull(1, java.sql.Types.INTEGER);
			} else {
				ps1.setInt(1, processId);
			}
			ps1.setInt(2, intIdConfig);
			ps1.setString(3, status);
			ps1.setString(4, status);
			ps1.execute();
			ResultSet re = ps1.getGeneratedKeys();
			if (re.next()) {
				return re.getInt(1);
			}

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
		return -1;
	}

}
