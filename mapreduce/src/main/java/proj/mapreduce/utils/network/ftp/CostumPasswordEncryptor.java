package proj.mapreduce.utils.network.ftp;

import org.apache.ftpserver.usermanager.PasswordEncryptor;


public class CostumPasswordEncryptor implements PasswordEncryptor {

	//We store clear-text passwords in this example

	@Override
	public String encrypt(String password) {
		return password;
	}

	@Override
	public boolean matches(String passwordToCheck, String storedPassword) {
		return passwordToCheck.equals(storedPassword);
	}
}
