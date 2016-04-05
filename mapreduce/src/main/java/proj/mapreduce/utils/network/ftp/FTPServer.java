package proj.mapreduce.utils.network.ftp;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

public class FTPServer {

	UserManager m_usermanager = null;
	FtpServer m_ftpserver = null;

	public void createFtpServer(int ftp_port) {
		Map<String, Ftplet> ftplet;
		FtpServerFactory serverFactory;
		ListenerFactory factory;

		factory = new ListenerFactory();
		factory.setPort(ftp_port);

		serverFactory = new FtpServerFactory();
		serverFactory.addListener("default", factory.createListener());

		if (m_usermanager == null) {
			createUserManager();
			addUser("test", "test", "/home/test");
		}
		serverFactory.setUserManager(m_usermanager);

		ftplet = new HashMap<String, Ftplet>();
		ftplet.put("miaFtplet", new CostumFtplet());
		serverFactory.setFtplets(ftplet);

		m_ftpserver = serverFactory.createServer();
	}

	public void createUserManager() {
		PropertiesUserManagerFactory userManagerFactory;

		userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File(System.getProperty("user.dir")
				+ "/myusers.properties"));
		userManagerFactory.setPasswordEncryptor(new CostumPasswordEncryptor());

		m_usermanager = userManagerFactory.createUserManager();
	}

	public void createUserManager(String path) {
		PropertiesUserManagerFactory userManagerFactory;

		userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File(path));
		userManagerFactory.setPasswordEncryptor(new CostumPasswordEncryptor());

		m_usermanager = userManagerFactory.createUserManager();
	}

	public void addUser(String username, String password, String homedir) {
		List<Authority> authorities = new ArrayList<Authority>();
		BaseUser user = new BaseUser();

		authorities.add(new WritePermission());

		user.setName(username);
		user.setPassword(password);
		user.setHomeDirectory(homedir);
		user.setAuthorities(authorities);

		try {
			m_usermanager.save(user);
		} catch (FtpException e1) {
			;
		}

	}

	public void start() {
		try {
			m_ftpserver.start();
		} catch (FtpException ex) {
			;
		}
	}
}
