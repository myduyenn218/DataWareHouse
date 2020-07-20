package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Config {
	// Các trường của Loan
	private int idConf;
	private String configName;// Trường này là tên của config ứng với kiểu file
								// vd:f_txt.
	private String url_db_staging;
	private String table_name_staging;
	private String url_db_warehouse;
	private String table_name_warehouse;
	private String path_file_local;
	private String username_download;
	private String password_download;
	private String remote_file;
	private String type_file;
	private String username_db;
	private String password_db;
	private String fields;
	// Trường này trở đi là có trong config của Phượng
//	private String targetTable;
//	private String fileType;
//	private String importDir;
//	private String successDir;
//	private String errorDir;
//	private String variabless;

	public Config() {

	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public int getIdConf() {
		return idConf;
	}

	public void setIdConf(int idConf) {
		this.idConf = idConf;
	}

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public String getUrl_db_staging() {
		return url_db_staging;
	}

	public void setUrl_db_staging(String url_db_staging) {
		this.url_db_staging = url_db_staging;
	}

	public String getTable_name_staging() {
		return table_name_staging;
	}

	public void setTable_name_staging(String table_name_staging) {
		this.table_name_staging = table_name_staging;
	}

	public String getUrl_db_warehouse() {
		return url_db_warehouse;
	}

	public void setUrl_db_warehouse(String url_db_warehouse) {
		this.url_db_warehouse = url_db_warehouse;
	}

	public String getTable_name_warehouse() {
		return table_name_warehouse;
	}

	public void setTable_name_warehouse(String table_name_warehouse) {
		this.table_name_warehouse = table_name_warehouse;
	}

	public String getPath_file_local() {
		return path_file_local;
	}

	public void setPath_file_local(String path_file_local) {
		this.path_file_local = path_file_local;
	}

	public String getUsername_download() {
		return username_download;
	}

	public void setUsername_download(String username_download) {
		this.username_download = username_download;
	}

	public String getPassword_download() {
		return password_download;
	}

	public void setPassword_download(String password_download) {
		this.password_download = password_download;
	}

	public String getRemote_file() {
		return remote_file;
	}

	public void setRemote_file(String remote_file) {
		this.remote_file = remote_file;
	}

	public String getType_file() {
		return type_file;
	}

	public void setType_file(String type_file) {
		this.type_file = type_file;
	}

	public String getUsername_db() {
		return username_db;
	}

	public void setUsername_db(String username_db) {
		this.username_db = username_db;
	}

	public String getPassword_db() {
		return password_db;
	}

	public void setPassword_db(String password_db) {
		this.password_db = password_db;
	}

	@Override
	public String toString() {
		return "Config [idConf=" + idConf + ", configName=" + configName + ", url_db_staging=" + url_db_staging
				+ ", table_name_staging=" + table_name_staging + ", url_db_warehouse=" + url_db_warehouse
				+ ", table_name_warehouse=" + table_name_warehouse + ", path_file_local=" + path_file_local
				+ ", username_download=" + username_download + ", password_download=" + password_download
				+ ", remote_file=" + remote_file + ", type_file=" + type_file + ", username_db=" + username_db
				+ ", password_db=" + password_db + ", fields=" + fields + "]";
	}

}
