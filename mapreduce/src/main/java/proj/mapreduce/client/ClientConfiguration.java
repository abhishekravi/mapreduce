
package proj.mapreduce.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import proj.mapreduce.job.Job;
import proj.mapreduce.server.ClientObserver;

/**
 * class that holds client configuration.
 * 
 * @author all team
 *
 */
public class ClientConfiguration {

	boolean active = false;
	InetAddress address;
	public final static int serverport = 9182;
	public final static int ftpport = 8291;
	String ftpuser;
	String ftppass;
	String ftppath;
	ClientObserver observer;
	Job assinedjob;

	/**
	 * set FTP parameters/
	 * 
	 * @param user
	 * @param password
	 * @param path
	 */
	public void setupFtpConfiguration(String user, String password, String path) {
		this.ftpuser = user;
		this.ftppass = password;
		this.ftppath = path;
	}

	/**
	 * set address using ip address.
	 * 
	 * @param ipaddr
	 *            ipaddress
	 */
	public void setIpaddressbyName(String ipaddr) {
		try {
			this.address = InetAddress.getByName(ipaddr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public InetAddress getAddress() {
		return address;
	}

	public void setAddress(InetAddress address) {
		this.address = address;
	}

	public String getFtpuser() {
		return ftpuser;
	}

	public void setFtpuser(String ftpuser) {
		this.ftpuser = ftpuser;
	}

	public String getFtppass() {
		return ftppass;
	}

	public void setFtppass(String ftppass) {
		this.ftppass = ftppass;
	}

	public String getFtppath() {
		return ftppath;
	}

	public void setFtppath(String ftppath) {
		this.ftppath = ftppath;
	}

	public ClientObserver getObserver() {
		return observer;
	}

	public void setObserver(ClientObserver observer) {
		this.observer = observer;
	}

	public Job getAssinedjob() {
		return assinedjob;
	}

	public void setAssinedjob(Job assinedjob) {
		this.assinedjob = assinedjob;
	}

}
