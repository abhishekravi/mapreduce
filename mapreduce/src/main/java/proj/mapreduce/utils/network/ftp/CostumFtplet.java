package proj.mapreduce.utils.network.ftp;

import java.io.IOException;

import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;


public class CostumFtplet implements Ftplet {
	
	@Override
	public void init(FtpletContext ftpletContext) throws FtpException {
		//System.out.println("init");
		//System.out.println("Thread #" + Thread.currentThread().getId());
	}

	@Override
	public void destroy() {
		//System.out.println("destroy");
		//System.out.println("Thread #" + Thread.currentThread().getId());
	}

	@Override
	public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws FtpException, IOException
	{
		//System.out.println("beforeCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine());
		//System.out.println("Thread #" + Thread.currentThread().getId());

		//do something
		return FtpletResult.DEFAULT;//...or return accordingly
	}

	@Override
	public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) throws FtpException, IOException
	{
		//System.out.println("afterCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine() + " | " + reply.getMessage() + " : " + reply.toString());
		//System.out.println("Thread #" + Thread.currentThread().getId());

		//do something
		return FtpletResult.DEFAULT;//...or return accordingly
	}

	@Override
	public FtpletResult onConnect(FtpSession session) throws FtpException, IOException
	{
		//System.out.println("onConnect " + session.getUserArgument() + " : " + session.toString());
		//System.out.println("Thread #" + Thread.currentThread().getId());

		//do something
		return FtpletResult.DEFAULT;//...or return accordingly
	}

	@Override
	public FtpletResult onDisconnect(FtpSession session) throws FtpException, IOException
	{
		//System.out.println("onDisconnect " + session.getUserArgument() + " : " + session.toString());
		//System.out.println("Thread #" + Thread.currentThread().getId());

		//do something
		return FtpletResult.DEFAULT;//...or return accordingly
	}

	
}
