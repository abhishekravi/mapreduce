/**
 * Class to download file from FTP server.
 * @inspired from: www.codejava.net
 * @author: 
 */

package proj.mapreduce.utils.network.ftp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;


public class FtpClient {

	FTPClient ftpClient;
	
	public FtpClient (String server, int port, String user, String password) throws SocketException, IOException
	{
		ftpClient = new FTPClient();
		
		ftpClient.connect(server, port);
		ftpClient.login(user, password);
		ftpClient.enterLocalPassiveMode();
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

	}
	
	public boolean downloadFileBlocking (String severfile, String localfile) throws IOException
	{
		boolean result = false;

		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localfile));
		result = ftpClient.retrieveFile(severfile, outputStream);
		
		outputStream.close();
		
		return result;
	}

	public boolean downloadFileNonBlocking (String serverfile, String localfile) throws IOException
	{
		int bytesRead = -1;
		boolean result = false;
		byte[] bytesArray = new byte[4096];
		InputStream inputStream = ftpClient.retrieveFileStream(serverfile);
		
		File dwFile = new File(localfile);
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dwFile));
		
		while ((bytesRead = inputStream.read(bytesArray)) != -1) {
			outputStream.write(bytesArray, 0, bytesRead);
		}

		result = ftpClient.completePendingCommand();
		
		outputStream.close();
		inputStream.close();
		
		return result;
	}

	public void stop () throws IOException
	{
		if (ftpClient.isConnected()) 
		{
			ftpClient.logout();
			ftpClient.disconnect();
		}
	}
}
