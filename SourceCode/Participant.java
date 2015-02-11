/************************************************************
Class : Participant.java
This is the main class for one participant of the Paxos consensus algorithm.
It is started by the Paxos driver and initializes appropriate sockets.
The driver tells the class whether it should be a coordinator or not.

Roles:
-------
Participant = Proposer + Acceptor + (maybe Co-ordinator).
The Participant spawns seperate threads that execute the roles independently.
*************************************************************/

import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

public class Participant
{
	// General participant information
	public static String name;
	public static int id;
	public static boolean isCoordinator;
	public static int totalParticipants;
	public static int driverListenPort = 19998;
	public static int coordListenPort = 19999;
	public static int startAcceptorListenPort = 20000;
	public static int backlog = Integer.MAX_VALUE;

	//Proposer specific data structures
	public static Proposer proposer;

	//Acceptor specific data structures
	public static Acceptor acceptor;

	//Co-ordinator specific data structures
	public static Coordinator coordinator;

	//Logging
	public static FileOutputStream out;
	public static PrintStream p;

	private static void InitializeParticipant(String[] args) throws FileNotFoundException
	{
		name = args[0];
		id = Integer.parseInt(args[1]);
		isCoordinator = new Boolean(args[2]);
		totalParticipants = Integer.parseInt(args[3]);

		out = new FileOutputStream(new File(name + ".out"));
		p = new PrintStream(out);

		p.println("Starting " + name);
		//initialize proposer specific data
		proposer.Initialize(name, id, coordListenPort, driverListenPort, p);

		//initialize acceptor specific data
		acceptor.Initialize(name, id, coordListenPort, startAcceptorListenPort + id, p);

		//initialize coordinator specific data
		coordinator.Initialize(name, id, totalParticipants, coordListenPort, startAcceptorListenPort, p);
	}

	public static void main(String[] args) throws IOException, FileNotFoundException, InterruptedException
	{
		InitializeParticipant(args);

		//if coordinator, start coordinator thread that listens for connection
		if (isCoordinator)
		{
			coordinator.ExecuteRole();
		}

		Thread.sleep(totalParticipants * 1000);
		//start acceptor thread that listens for connection
		acceptor.ExecuteRole();

		Thread.sleep(totalParticipants * 1000);
		//start proposer role
		proposer.ExecuteRole();
	}
}
