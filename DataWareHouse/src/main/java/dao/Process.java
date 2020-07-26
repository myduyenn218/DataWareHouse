package dao;

public class Process {
	private int processId;
	private int process_config_id;
	private String process_status;
	private String dt_process_queued;
	private String dt_process_completed;
	private String dt_lastchange;

	public Process() {
		// TODO Auto-generated constructor stub
	}

	public Process(int processId) {
		super();
		this.processId = processId;
	}

	public int getProcessId() {
		return processId;
	}

	public void setProcessId(int processId) {
		this.processId = processId;
	}

	public int getProcess_config_id() {
		return process_config_id;
	}

	public void setProcess_config_id(int process_config_id) {
		this.process_config_id = process_config_id;
	}

	public String getProcess_status() {
		return process_status;
	}

	public void setProcess_status(String process_status) {
		this.process_status = process_status;
	}

	public String getDt_process_queued() {
		return dt_process_queued;
	}

	public void setDt_process_queued(String dt_process_queued) {
		this.dt_process_queued = dt_process_queued;
	}

	public String getDt_process_completed() {
		return dt_process_completed;
	}

	public void setDt_process_completed(String dt_process_completed) {
		this.dt_process_completed = dt_process_completed;
	}

	public String getDt_lastchange() {
		return dt_lastchange;
	}

	public void setDt_lastchange(String dt_lastchange) {
		this.dt_lastchange = dt_lastchange;
	}

}
