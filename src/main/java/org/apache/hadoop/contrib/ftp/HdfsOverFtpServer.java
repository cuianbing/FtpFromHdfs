package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.log4j.Logger;

/**
 * FTP服务启动
 */
public class HdfsOverFtpServer {

	private static Logger log = Logger.getLogger(HdfsOverFtpServer.class);

	private static int port = 0;
	private static int sslPort = 0;
	private static String passivePorts = null;
	private static String sslPassivePorts = null;
	private static String hdfsUri = null;

	public static void main(String[] args) throws Exception {
		loadConfig();

		if (port != 0) {
			startServer();
		}

		if (sslPort != 0) {
			startSSLServer();
		}
	}

	/**
	 * Load configuration
	 *
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(loadResource("/hdfs-over-ftp.properties")));

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set. so ftp server will not be started");
		}

		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("ssl-port is set. ssl server will be started");
		} catch (Exception e) {
			log.info("ssl-port is not set. so ssl server will not be started");
		}

		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.fatal("data-ports is not set");
				System.exit(1);
			}
		}

		if (sslPort != 0) {
			sslPassivePorts = props.getProperty("ssl-data-ports");
			if (sslPassivePorts == null) {
				log.fatal("ssl-data-ports is not set");
				System.exit(1);
			}
		}

		hdfsUri = props.getProperty("hdfs-uri");
		if (hdfsUri == null) {
			log.fatal("hdfs-uri is not set");
			System.exit(1);
		}

		String superuser = props.getProperty("superuser");
		if (superuser == null) {
			log.fatal("superuser is not set");
			System.exit(1);
		}
		HdfsOverFtpSystem.setSuperuser(superuser);
	}

	/**
	 * Starts FTP server
	 *
	 * @throws Exception
	 */
	public static void startServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp server. port: " + port + " data-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		factory.setPort(2221);
		serverFactory.addListener("default", factory.createListener());
		FtpServer server = serverFactory.createServer();         
		server.start();
		/**************************		原始代码
		FtpServer server = new FtpServer();

		//数据连接配置接口。
		DataConnectionConfiguration dataCon = new DefaultDataConnectionConfiguration(port, null, false, false, hdfsUri, port, hdfsUri, null, hdfsUri, false);
		//设置允许的被动端口。
		dataCon.setPassivePorts(passivePorts);
		//为在此侦听器中进行的数据连接设置配置
		server.getListener("default").setDataConnectionConfiguration(dataCon);
		//设置侦听器将在其上接受请求的端口。或者设置为0(0)是端口应该自动分配
		server.getListener("default").setPort(port);


		HdfsUserManager userManager = new HdfsUserManager();
		final File file = loadResource("/users.properties");

		userManager.setFile(file);

		server.setUserManager(userManager);

		server.setFileSystem(new HdfsFileSystemManager());
		server.start();
		 *********************/
		
	}

	private static File loadResource(String resourceName) {
		final URL resource = HdfsOverFtpServer.class.getResource(resourceName);
		if (resource == null) {
			throw new RuntimeException("Resource not found: " + resourceName);
		}
		return new File(resource.getFile());
	}

	/**
	 * 启动SSL FTP服务器
	 *
	 * @throws Exception
	 */
	public static void startSSLServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp SSL server. ssl-port: " + sslPort + " ssl-data-ports: " + sslPassivePorts + " hdfs-uri: " + hdfsUri);


		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);
		/***
		FtpServer server = new FtpServer();

		DataConnectionConfiguration dataCon = new DefaultDataConnectionConfiguration();
		dataCon.setPassivePorts(sslPassivePorts);
		server.getListener("default").setDataConnectionConfiguration(dataCon);
		server.getListener("default").setPort(sslPort);

		MySslConfiguration ssl = new MySslConfiguration();
		ssl.setKeystoreFile(new File("ftp.jks"));
		ssl.setKeystoreType("JKS");
		ssl.setKeyPassword("333333");
		server.getListener("default").setSslConfiguration(ssl);
		server.getListener("default").setImplicitSsl(true);


		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(new File("users.conf"));

		server.setUserManager(userManager);

		server.setFileSystem(new HdfsFileSystemManager());


		server.start();
		**/
		
		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		// set the port of the listener
		factory.setPort(2221);
		// define SSL configuration
		SslConfigurationFactory ssl = new SslConfigurationFactory();
		ssl.setKeystoreFile(new File("src/test/resources/ftpserver.jks"));
		ssl.setKeystorePassword("password");
		// set the SSL configuration for the listener
		factory.setSslConfiguration(ssl.createSslConfiguration());
		factory.setImplicitSsl(true);
		// replace the default listener
		serverFactory.addListener("default", factory.createListener());
		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File("myusers.properties"));
		serverFactory.setUserManager(userManagerFactory.createUserManager());
		// start the server
		FtpServer server = serverFactory.createServer(); 
		server.start();
	}
}
