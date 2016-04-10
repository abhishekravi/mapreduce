package proj.mapreduce.utils.network.ftp;

import java.io.File;
import java.net.URISyntaxException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import proj.mapreduce.client.ClientConfiguration;

public class FTPServer {

	private static Logger LOGGER = LoggerFactory.getLogger(FTPServer.class);
	UserManager usermanager;
	FtpServer ftpserver;
	ClientConfiguration clientconf;

	public FTPServer(ClientConfiguration clientconf) {
		this.clientconf = clientconf;
		usermanager = null;
		ftpserver = null;
	}

	public void createFtpServer() {
		Map<String, Ftplet> ftplet;
		FtpServerFactory serverFactory;
		ListenerFactory factory;

		factory = new ListenerFactory();
		factory.setPort(ClientConfiguration.ftpport);

		serverFactory = new FtpServerFactory();
		serverFactory.addListener("default", factory.createListener());

		if (usermanager == null) {
			createUserManager();
			addUser(clientconf.getFtpuser(), clientconf.getFtppass(), clientconf.getFtppath());
		}
		serverFactory.setUserManager(usermanager);

		ftplet = new HashMap<String, Ftplet>();
		ftplet.put("miaFtplet", new CostumFtplet());
		serverFactory.setFtplets(ftplet);

		ftpserver = serverFactory.createServer();
	}

	public void createUserManager() {
		PropertiesUserManagerFactory userManagerFactory;

		userManagerFactory = new PropertiesUserManagerFactory();
		try {
			File propFile = new File(FTPServer.class.getClassLoader().getResource("users.properties").toURI());
			userManagerFactory.setFile(propFile);
		} catch (URISyntaxException e) {
			LOGGER.error("error loading user.properties::exception-" + e.getMessage());
		}
		userManagerFactory.setPasswordEncryptor(new CostumPasswordEncryptor());

		usermanager = userManagerFactory.createUserManager();
	}

	public void createUserManager(String path) {
		PropertiesUserManagerFactory userManagerFactory;

		userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File(path));
		userManagerFactory.setPasswordEncryptor(new CostumPasswordEncryptor());

		usermanager = userManagerFactory.createUserManager();
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
			usermanager.save(user);
		} catch (FtpException e1) {
			;
		}

	}

	public void start() {
		try {
			ftpserver.start();
		} catch (FtpException ex) {
			;
		}
	}
}
