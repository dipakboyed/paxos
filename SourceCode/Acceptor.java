import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

public class Acceptor
{
	// General participant information
	private static String name;
	private static int id;
	private static int coordListenPort;
	private static int acceptorListenPort;
	private static int backlog = Integer.MAX_VALUE;

	//Acceptor specific data structures
	private static int highestProposalAcked;
	private static char valueAccepted;
	private static int proposalNumberAccepted;

	private static PrintStream p;

	public static void Initialize(String acceptorName, int acceptorId, int coordPort, int listenPort, PrintStream ps)
	{
		name = acceptorName;
		id = acceptorId;
		coordListenPort = coordPort;
		acceptorListenPort = listenPort;
		p = ps;

		//initialize acceptor specific data
		highestProposalAcked = -1;
		valueAccepted = '0';
		proposalNumberAccepted = -1;
	}

	public static void ExecuteRole()
	{
		new AcceptorThread().start();
	}

	private static class AcceptorThread extends Thread
	{
		public void run()
		{
			boolean listening = true;
			try
			{
				ServerSocket acceptorSocket = new ServerSocket(acceptorListenPort, backlog);
				LogMsg("Starting to listen on port " + acceptorListenPort);
				while (listening)
				{
					HandleIncomingMsg(acceptorSocket.accept());
				}
				acceptorSocket.close();
			}
			catch (IOException e)
			{
				LogMsg("Could not listen on port: " + acceptorListenPort);
				System.exit(-1);
			}
		}
	}

	private synchronized static void HandleIncomingMsg(Socket socket)
	{
		try
		{
			BufferedReader socketIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String inputLine = socketIn.readLine();
			LogMsg("Received  message: " + inputLine);
			String[] parseIncomingMsg = inputLine.split(";");

			//parse incoming message and take appropriate action
			if (parseIncomingMsg[0].equals("MSG_PREPAREREQUEST"))
			{
				//Msg Format : MSG_PREPAREREQUEST;<proposerId>;<n>
				int proposerId = Integer.parseInt(parseIncomingMsg[1]);
				int currentProposalNumber = Integer.parseInt(parseIncomingMsg[2]);
				if (currentProposalNumber > highestProposalAcked)
				{
					//Respond with promise not to accept any more proposals < currentProposalNumber
					try
					{
						highestProposalAcked = currentProposalNumber;

						Socket coordSocket = new Socket(InetAddress.getLocalHost(), coordListenPort);
						PrintWriter coordSocketOut = new PrintWriter(coordSocket.getOutputStream(), true);
						String message = "MSG_ACKPREPAREREQUEST;" + proposerId + ";" + currentProposalNumber + ";" + id + ";" + proposalNumberAccepted + ";" + valueAccepted;
						LogMsg("Sending '" + message + "' to coordinator");
						coordSocketOut.println(message);
					}
					catch (UnknownHostException e) { e.printStackTrace(p); }
					catch (IOException ioE) { ioE.printStackTrace(p); }
				}
			}
			else if (parseIncomingMsg[0].equals("MSG_ACCEPT"))
			{
				//Msg Format: MSG_ACCEPT;<proposerId>;<n>;<v>
				int proposerId = Integer.parseInt(parseIncomingMsg[1]);
				int currentProposalNumber = Integer.parseInt(parseIncomingMsg[2]);
				char currentValue = parseIncomingMsg[3].charAt(0);

				if (highestProposalAcked <= currentProposalNumber)
				{
					//accept proposal
					valueAccepted = currentValue;
					proposalNumberAccepted = currentProposalNumber;
					//Respond with ack accept
					try
					{
						Socket coordSocket = new Socket(InetAddress.getLocalHost(), coordListenPort);
						PrintWriter coordSocketOut = new PrintWriter(coordSocket.getOutputStream(), true);
						String message = "MSG_ACKACCEPT;" + proposerId + ";" + id + ";" + currentProposalNumber + ";" + currentValue;
						LogMsg("Sending '" + message + "'to coordinator");
						coordSocketOut.println(message);
					}
					catch (UnknownHostException e) { e.printStackTrace(p); }
					catch (IOException ioE) { ioE.printStackTrace(p); }
				}
			}
		}
		catch (IOException e) { e.printStackTrace(p); }
	}

	private synchronized static void LogMsg(String message)
	{
		p.println(name + ": Acceptor : " + message);
	}
}
