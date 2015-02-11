import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

public class Proposer
{
	// General participant information
	private static String name;
	private static int id;
	private static int driverListenPort;
	private static int coordListenPort;
	private static int backlog = Integer.MAX_VALUE;

	//Proposer specific data structures
	private static int currentProposalNumber;
	private static Vector<Character> allowableValues;
	//private static boolean valueAccepted;
	private static boolean reachedConsensus;
	private static char consensusValue;
	private static boolean waitForProposalReply;
	private static Timer livenessTimer;

	private static PrintStream p;

	public static void Initialize(String proposerName, int proposerId, int coordPort, int driverPort, PrintStream ps)
	{
		name = proposerName;
		id = proposerId;
		driverListenPort = driverPort;
		coordListenPort = coordPort;
		p = ps;

		//initialize proposer specific data
		currentProposalNumber = -1;
		char c = 'a';
		allowableValues = new Vector<Character>(26);
		for (int i = 0; i < 26; i++, c++)
		{
			allowableValues.add(i, c);
		}
		reachedConsensus = false;
		consensusValue = '0';
		waitForProposalReply = false;
		livenessTimer = new Timer();
	}

	public synchronized static void ExecuteRole()
	{
		Socket coordSocket = null;
		while (!reachedConsensus)
		{
			//need to create a new proposal, get latest proposal number
			currentProposalNumber = GetNextProposalNumber();
			if (currentProposalNumber < 0)
			{
				LogMsg("Driver returned illegal proposal number");
				System.exit(-1);
			}

			//select value to be proposed
			int index = (new Random()).nextInt(allowableValues.size() - 1);
			char value = allowableValues.get(index);

			//send proposal and wait for reply
			try
			{
				coordSocket = new Socket(InetAddress.getLocalHost(), coordListenPort);
				StartProposal(coordSocket, currentProposalNumber, value);
				coordSocket.close();
			}
			catch (UnknownHostException e)
			{
				LogMsg("Don't know about localhost");
				System.exit(-1);
			}
			catch (IOException e)
			{
				LogMsg("Couldn't get I/O for the connection to coordinator.");
				e.printStackTrace(p);
				System.exit(-1);
			}
		}

		LogMsg("Participant has reached a consensus on value: " + consensusValue);
		NotifyDriverOfConsensus();
		System.exit(0);
	}

	private synchronized static void NotifyDriverOfConsensus()
	{
		try
		{
			Socket driverSocket = new Socket(InetAddress.getLocalHost(), driverListenPort);
			PrintWriter driverSocketOut = new PrintWriter(driverSocket.getOutputStream(), true);
			LogMsg("Sending consensus information to driver");
			driverSocketOut.println("Consensus;" + consensusValue);
			driverSocketOut.close();
			driverSocket.close();
		}
		catch (UnknownHostException e)
		{
			LogMsg("Don't know about localhost");
			System.exit(-1);
		}
		catch (IOException e)
		{
			LogMsg("Couldn't get I/O for the connection to driver.");
			System.exit(-1);
		}
	}

	private synchronized static int GetNextProposalNumber()
	{
		int returnValue = -1;
		try
		{
			Socket driverSocket = new Socket(InetAddress.getLocalHost(), driverListenPort);
			PrintWriter driverSocketOut = new PrintWriter(driverSocket.getOutputStream(), true);
			BufferedReader driverSocketIn = new BufferedReader(new InputStreamReader(driverSocket.getInputStream()));
			LogMsg("Requesting new n from driver");
			driverSocketOut.println("GetNewN");
			boolean getReply = false;
			String Reply;
			while (!getReply)
			{
				if ((Reply = driverSocketIn.readLine()) != null)
				{
					LogMsg("Received new n=" + Reply + "from driver");
					returnValue = Integer.parseInt(Reply);
					getReply = true;
				}
			}
			driverSocketOut.close();
			driverSocketIn.close();
			driverSocket.close();
		}
		catch (UnknownHostException e)
		{
			LogMsg("Don't know about localhost");
			System.exit(-1);
		}
		catch (IOException e)
		{
			LogMsg("Couldn't get I/O for the connection to driver.");
			System.exit(-1);
		}
		return returnValue;
	}

	private synchronized static void StartProposal(Socket coordSocket, int proposalNumber, char value)
	{
		try
		{
			PrintWriter coordSocketOut = new PrintWriter(coordSocket.getOutputStream(), true);
			BufferedReader coordSocketIn = new BufferedReader(new InputStreamReader(coordSocket.getInputStream()));
			String message = "MSG_PROPOSAL;" + id + ";" + proposalNumber + ";" + value;
			LogMsg("Sending new proposal (" + proposalNumber + "," + value + ") to coordinator");
			coordSocketOut.println(message);
			String Reply;
			waitForProposalReply = true;
			livenessTimer.schedule(new ProposerLivenessTimerTask(), 1 * 60 * 1000);

			while (waitForProposalReply)
			{
				// wait for reply or liveness timer to expire
				if ((Reply = coordSocketIn.readLine()) != null)
				{
					HandleIncomingMsg(Reply);
					waitForProposalReply = false;
				}
			}
			coordSocketOut.close();
			coordSocketIn.close();
		}
		catch (IOException e)
		{
			LogMsg("Couldn't get I/O for the connection to localhost.");
			e.printStackTrace(p);
			System.exit(-1);
		}
	}

	private static class ProposerLivenessTimerTask extends TimerTask
	{
		public void run()
		{
			LogMsg("Proposer timer expired...");
			waitForProposalReply = false;
		}
	}

	private synchronized static void HandleIncomingMsg(String reply)
	{
		// this may updated the following data structures based on the reply
		// allowableValues, reachedConsensus, consensusValue
		// This can receive MSG_REPLYPROPOSAL OR MSG_CONSENSUS
		// Msg Format :  "MSG_REPLYPROPOSAL;<proposerId>;<n>; FAIL; <v to propose next>
		//	      :  "MSG_CONSENSUS;<v>"

		LogMsg("Received from coordinator '" + reply + "'");
		livenessTimer.cancel();
		String[] parseIncomingMsg = reply.split(";");

		//parse incoming message and take appropriate action
		if (parseIncomingMsg[0].equals("MSG_REPLYPROPOSAL"))
		{
			boolean status = parseIncomingMsg[3].equals("PASS");
			if (!status)
			{
				//proposal failed, update allowableValues
				char futureValueToPropose = parseIncomingMsg[4].charAt(0);
				allowableValues.clear();
				allowableValues.add(futureValueToPropose);
			}
		}
		else if (parseIncomingMsg[0].equals("MSG_CONSENSUS"))
		{
			reachedConsensus = true;
			consensusValue = parseIncomingMsg[1].charAt(0);
		}
	}

	private synchronized static void LogMsg(String message)
	{
		p.println(name + ": Proposer : " + message);
	}
}
