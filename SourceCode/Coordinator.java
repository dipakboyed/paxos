import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

public class Coordinator
{
	// General participant information
	private static String name;
	private static int id;
	private static int totalParticipants;
	private static int coordListenPort;
	private static int startAcceptorListenPort;
	private static int backlog = Integer.MAX_VALUE;

	//Co-ordinator specific data structures
	//Phase 1 structures
	private static Vector<Socket> proposerReplySocket;
	private static Vector<Character> proposedValue;
	private static Vector<Integer> proposalNumber;

	// Phase 1 reply structures
	private static Vector<Integer> highestProposalNumberFromAcceptor;
	private static Vector<Character> highestValueFromAcceptor;
	private static Vector<Vector<Integer>> receivedAcceptorAcksFrom;

	// Phase 2 reply structures
	private static Vector<Boolean> hasAcceptorReachedConsensus;
	private static Vector<Character> valueReachedByAcceptor;
	private static boolean consensusReached;
	private static char consensusValue;

	//Logging
	private static PrintStream p;

	public static void Initialize(String coordName, int coordId, int total, int coordPort, int startAcceptorPort, PrintStream ps)
	{
		name = coordName;
		id = coordId;
		totalParticipants = total;
		coordListenPort = coordPort;
		startAcceptorListenPort = startAcceptorPort;
		p = ps;

		//initialize coordinator specific data
		proposerReplySocket = new Vector<Socket>(totalParticipants);
		proposedValue = new Vector<Character>(totalParticipants);
		proposalNumber = new Vector<Integer>(totalParticipants);

		highestProposalNumberFromAcceptor = new Vector<Integer>(totalParticipants);
		highestValueFromAcceptor = new Vector<Character>(totalParticipants);
		receivedAcceptorAcksFrom = new Vector<Vector<Integer>>(totalParticipants);

		hasAcceptorReachedConsensus = new Vector<Boolean>(totalParticipants);
		valueReachedByAcceptor = new Vector<Character>(totalParticipants);
		consensusReached = false;
		consensusValue = '0';

		for (int i = 0; i < totalParticipants; i++)
		{
			proposerReplySocket.add(i, null);
			proposedValue.add(i, '0');
			proposalNumber.add(i, -1);

			highestProposalNumberFromAcceptor.add(i, -1);
			highestValueFromAcceptor.add(i, '0');
			receivedAcceptorAcksFrom.add(i, new Vector<Integer>(totalParticipants));

			hasAcceptorReachedConsensus.add(i, false);
			valueReachedByAcceptor.add(i, '0');
		}
	}

	public static void ExecuteRole()
	{
		new CoordThread().start();
	}

	private static class CoordThread extends Thread
	{
		public void run()
		{
			ServerSocket coordSocket = null;
			boolean listening = true;
			try
			{
				coordSocket = new ServerSocket(coordListenPort, backlog);
				LogMsg("Starting to listen on port " + coordListenPort);
				while (listening)
				{
					HandleIncomingMsg(coordSocket.accept());
				}
				coordSocket.close();
			}
			catch (IOException e)
			{
				LogMsg("Could not listen on port: " + coordListenPort);
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
			LogMsg("Received message: " + inputLine);
			String[] parseIncomingMsg = inputLine.split(";");

			//parse incoming message and take appropriate action

			if (parseIncomingMsg[0].equals("MSG_PROPOSAL"))
			{
				// Msg Format : MSG_PROPOSAL;<proposerId>;<n>;<v>

				int index = Integer.parseInt(parseIncomingMsg[1]);
				int pNumber = Integer.parseInt(parseIncomingMsg[2]);
				char vProposed = parseIncomingMsg[3].charAt(0);

				if (consensusReached && vProposed != consensusValue)
				{
					//proposer's value cannot be accepted, inform proposer
					try
					{
						PrintWriter proposerSocketOut = new PrintWriter(socket.getOutputStream(), true);
						String message = "MSG_CONSENSUS;" + consensusValue;
						LogMsg("Sending '" + message + "' to proposer#" + index);
						proposerSocketOut.println(message);
					}
					catch (UnknownHostException e) { e.printStackTrace(p); }
					catch (IOException ioE) { ioE.printStackTrace(p); }
					catch (Exception ge) { /*proposerReplySocket may have been updated*/}
				}
				else
				{
					// overwrite any older proposal by same proposer
					proposerReplySocket.set(index, socket);
					proposalNumber.set(index, pNumber);
					proposedValue.set(index, vProposed);
					highestProposalNumberFromAcceptor.set(index, -1);
					highestValueFromAcceptor.set(index, '0');
					receivedAcceptorAcksFrom.get(index).clear();

					//send Phase 1 prepare request message to all acceptors
					for (int i = 0; i < totalParticipants; i++)
					{
						try
						{
							Socket acceptorSocket = new Socket(InetAddress.getLocalHost(), startAcceptorListenPort + i);
							PrintWriter acceptorSocketOut = new PrintWriter(acceptorSocket.getOutputStream(), true);
							String message = "MSG_PREPAREREQUEST;" + index + ";" + proposalNumber.get(index);
							LogMsg("Sending '" + message + "' to acceptor#" + i);
							acceptorSocketOut.println(message);
						}
						catch (UnknownHostException e) { e.printStackTrace(p); }
						catch (IOException ioE) { ioE.printStackTrace(p); }
					}
				}
			}
			else if (parseIncomingMsg[0].equals("MSG_ACKPREPAREREQUEST"))
			{
				// Msg Format : MSG_ACKPREPAREREQUEST;<proposerId>;<n>;<AcceptorId>;<MaxPNumberAccepted>;<valueAccepted>
				int index = Integer.parseInt(parseIncomingMsg[1]);
				int currentProposalNumber = Integer.parseInt(parseIncomingMsg[2]);
				int acceptorId = Integer.parseInt(parseIncomingMsg[3]);

				if (proposalNumber.get(index) == currentProposalNumber)
				{
					// verified its not an old msg, process it
					// update Phase 1 reply data structures
					int proposalFromAcceptor = Integer.parseInt(parseIncomingMsg[4]);
					char valueFromAcceptor = parseIncomingMsg[5].charAt(0);
					if (proposalFromAcceptor > highestProposalNumberFromAcceptor.get(index))
					{
						highestProposalNumberFromAcceptor.set(index, proposalFromAcceptor);
						highestValueFromAcceptor.set(index, valueFromAcceptor);
					}
					receivedAcceptorAcksFrom.get(index).add(acceptorId);

					// check if need to send Msg_Accept for Phase 2
					if (receivedAcceptorAcksFrom.get(index).size() >= (Math.floor(totalParticipants / 2) + 1))
					{
						//received acks from majority of acceptors, can send Phase 2
						if (!highestValueFromAcceptor.get(index).equals('0') &&
							!highestValueFromAcceptor.get(index).equals(proposedValue.get(index)))
						{
							//proposer's value cannot be accepted, inform proposer
							try
							{
								PrintWriter proposerSocketOut = new PrintWriter(proposerReplySocket.get(index).getOutputStream(), true);
								String message = "MSG_REPLYPROPOSAL;" + index + ";" + proposalNumber.get(index) + ";FAIL;" + highestValueFromAcceptor.get(index);
								LogMsg("Sending '" + message + "' to proposer#" + index);
								proposerSocketOut.println(message);

								//clear data structures
								proposedValue.set(index, '0');
								proposalNumber.set(index, -1);
								highestProposalNumberFromAcceptor.set(index, -1);
								highestValueFromAcceptor.set(index, '0');
								receivedAcceptorAcksFrom.get(index).clear();
							}
							catch (UnknownHostException e) { e.printStackTrace(p); }
							catch (IOException ioE) { ioE.printStackTrace(p); }
							catch (Exception ge) { /*proposerReplySocket may have been updated*/}
						}
						else
						{
							// Send Phase 2 to acceptors
							char valueToSend = (highestProposalNumberFromAcceptor.get(index) == -1) ? proposedValue.get(index) : highestValueFromAcceptor.get(index);
							if (valueToSend != proposedValue.get(index))
							{
								// assert failure
								LogMsg("ERROR: valueToSend for Phase2 '" + valueToSend + "' is not sme as proposed Value '" + proposedValue.get(index) + "'");
								System.exit(-1);
							}

							//send Phase 2 accept message to all acceptors that acknowledged
							for (int i = 0; i < receivedAcceptorAcksFrom.get(index).size(); i++)
							{
								try
								{
									int acceptorPort = startAcceptorListenPort + receivedAcceptorAcksFrom.get(index).get(i);
									Socket acceptorSocket = new Socket(InetAddress.getLocalHost(), acceptorPort);
									PrintWriter acceptorSocketOut = new PrintWriter(acceptorSocket.getOutputStream(), true);
									String message = "MSG_ACCEPT;" + index + ";" + proposalNumber.get(index) + ";" + valueToSend;
									LogMsg("Sending '" + message + "' to acceptor#" + receivedAcceptorAcksFrom.get(index).get(i));
									acceptorSocketOut.println(message);
								}
								catch (UnknownHostException e) { e.printStackTrace(p); }
								catch (IOException ioE) { ioE.printStackTrace(p); }
							}
						}
					}
				}
			}
			else if (parseIncomingMsg[0].equals("MSG_ACKACCEPT"))
			{
				// Msg Format : MSG_ACKACCEPT;<proposerId>;<AcceptorId>;<n>;<v>
				int index = Integer.parseInt(parseIncomingMsg[1]);
				int acceptorId = Integer.parseInt(parseIncomingMsg[2]);
				int currentpNumber = Integer.parseInt(parseIncomingMsg[3]);
				char valueAccepted = parseIncomingMsg[4].charAt(0);

				hasAcceptorReachedConsensus.set(acceptorId, true);
				valueReachedByAcceptor.set(acceptorId, valueAccepted);

				// check if majority of acceptors have reached a consensus
				int[] valuesAcceptedCount = new int[26];
				for (int i = 0; i < totalParticipants; i++)
				{
					if (hasAcceptorReachedConsensus.get(i))
					{
						char val = valueReachedByAcceptor.get(i);
						if (val >= 'a' && val <= 'z')
						{
							++valuesAcceptedCount[25 - ('z' - val)];
						}
					}
				}
				for (int i = 0; i < 26; i++)
				{
					if (valuesAcceptedCount[i] >= (Math.floor(totalParticipants / 2) + 1))
					{
						char v = 'a';
						v += i;
						LogMsg("Majority of acceptors have accepted a value : " + v);
						consensusReached = true;
						consensusValue = v;

						//inform all proposers
						for (int j = 0; j < totalParticipants; j++)
						{
							try
							{
								PrintWriter proposerSocketOut = new PrintWriter(proposerReplySocket.get(j).getOutputStream(), true);
								String message = "MSG_CONSENSUS;" + consensusValue;
								LogMsg("Sending '" + message + "' to participant#" + j);
								proposerSocketOut.println(message);
							}
							catch (UnknownHostException e) { e.printStackTrace(p); }
							catch (IOException ioE) { ioE.printStackTrace(p); }
							catch (Exception ge) { /*proposerReplySocket may have been updated*/}
						}
						break;
					}
				}
			}
		}
		catch (IOException e) { e.printStackTrace(p); }
	}

	private synchronized static void LogMsg(String message)
	{
		p.println(name + ": Coordinator : " + message);
	}
}
