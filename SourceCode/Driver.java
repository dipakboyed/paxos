/************************************************************
Class : PaxosDriver.java
This is the driver class for the Paxos Consensus Algorithm
It is starts the spawns the Participant processes.
The driver tells the class whether it should be a coordinator or not.
*************************************************************/

import java.net.*;
import java.io.*;
import java.util.*;
import java.lang.Integer;
import java.lang.Process;
public class Driver
{
	public static int numParticipants;
	public static int driverListenPort = 19998;
	public static int CurrentIndex = 0;
	public static int backlog = Integer.MAX_VALUE;
	public static List<Process> processList = new ArrayList<Process>();

	public static void PrintUsage()
	{
		System.out.println("Usage: java Driver numberParticipants");
		System.out.println("where:");
		System.out.println("	'numberParticipants' is an int (>0) representing the total Paxos participants");
	}

	public static void main(String[] args) throws IOException, FileNotFoundException
	{
		ServerSocket driverSocket = null;
		try
		{
			numParticipants = Integer.parseInt(args[0]);
			if (numParticipants <= 0)
			{
				PrintUsage();
				System.exit(-1);
			}
		}
		catch (Exception e)
		{
			PrintUsage();
			System.exit(-1);
		}

		//Start Paxos participant processes
		try
		{
			System.out.println("This program runs Paxos consensus algorithm amongst the participants to reach a consensus on a selecting one alphabet between 'a' and 'z'");
			System.out.println("Starting Paxos participant process : Participant#0");
			processList.add(new ProcessBuilder("java", "Participant", "Participant#0", "0", "true", Integer.toString(numParticipants)).start());

			//Start other (non-coordinator) participant processes
			for (int i = 1; i < numParticipants; i++)
			{
				String name = "Participant#" + i;
				System.out.println("Starting Paxos participant process : " + name);
				processList.add(new ProcessBuilder("java", "Participant", name, Integer.toString(i), "false", Integer.toString(numParticipants)).start());
			}
		}
		catch (IOException e)
		{
			System.err.println("Exception thrown while starting processes");
			System.exit(-1);
		}

		try
		{
			driverSocket = new ServerSocket(driverListenPort, backlog);
		}
		catch (IOException e)
		{
			System.err.println("Driver: Could not listen on port: " + driverListenPort);
			System.exit(-1);
		}

		System.out.println("Please wait while the participant processes intialize and execute...");
		while (true)
		{
			System.out.println("Driver is listening for connection...");
			new DriverRespondThread(driverSocket.accept()).start();
		}

	}
	static class DriverRespondThread extends Thread
	{
		private Socket socket = null;

		public DriverRespondThread(Socket socket)
		{
			super("DriverRespondThread");
			this.socket = socket;
		}

		public void run()
		{
			HandleIncomingMsg();
		}

		private synchronized void HandleIncomingMsg()
		{
			try
			{
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String inputLine, outputLine;
				//read input stream
				inputLine = in.readLine(); //"RequestCurrentIndex"
				System.out.println("incoming message: '" + inputLine);

				if (inputLine.equals("GetNewN"))
				{
					//outputmessage
					CurrentIndex++;
					outputLine = Integer.toString(CurrentIndex);
					System.out.println("Outgoing message: '" + outputLine);

					//send result
					PrintWriter sendSocketOut = new PrintWriter(socket.getOutputStream(), true);
					sendSocketOut.println(outputLine);
				}
				else
				{
					String[] parseIncomingMsg = inputLine.split(";");
					if (parseIncomingMsg[0].equals("Consensus"))
					{
						System.out.println("Consensus has been reached on value : " + parseIncomingMsg[1]);
						System.out.println("See '<participant name>.out' file(s) for participant-level logging information");
						for (int i = 0; i < processList.size(); i++)
						{
							processList.get(i).destroy();
						}
						System.exit(0);
					}
				}
			}
			catch (IOException e)
			{
				e.toString();
			}
		}
	}
}
