package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;
import java.net.UnknownHostException;
import java.util.Dictionary;
import java.util.Hashtable;
import comp512.utils.*;	
import java.util.LinkedList;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.io.*;	
import java.util.logging.*;
import java.time.*;

public class Paxos
{
	GCL gcl;
	FailCheck failCheck;
	Logger logger;
	/*
	* Static helper variables
	*/
	private static final int THREAD_SLEEP_MAX_MILLIS = 50;
	private static final int THREAD_SLEEP_MIN_MILLIS = 100;
	private static final long THREAD_POLLING_LOOP_SLEEP = 50;
	private static final long THREAD_TERMINATION_SLEEP = 1500;
	private static final String PAXOS_PHASE_PROPOSE_LEADER = "proposeleader";
	private static final String PAXOS_PHASE_PROPOSE_VALUE = "proposevalue";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE = "promiseacceptwithprevvalue";
	private static final String PAXOS_PHASE_PROMISE_DENY = "promisedeny";
	private static final String PAXOS_PHASE_CONFIRM_VALUE = "confirmvalue";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK = "acceptack";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_DENYACK = "denyack";
	
	private boolean shouldContinue = true;

	private Semaphore lock;
	private Random rand;
	/*
	* Helper variables meant for both proposers and acceptors.
	*/
	private String myProcess;
	private String[] allProcesses;
	private Thread paxosThread;
	private double currentHighestBallotID = 0.0;
	private TreeMap<Integer, Object> messagesTreeMap;

	/*
	* Private variables meant for the proposers/leaders
	*/
	private double currentProposerBallotID = 0.0;
	private double lastAcceptedBallotID;
	private Object lastAcceptedValue;
	private int processCount;
	private int upProcesses;
	private Dictionary<String, TriStateResponse> leaderPromises;
	private Dictionary<String, TriStateResponse> valuePromises;
	private Integer globalMessageCount = 0;
	private int processMessageCount = 0;
	private int currentProcessMoveNumber = 0;
	private int currentAcceptedMoveNumber = 0;
	private int playerNumber = -1;
	private Clock clk = Clock.systemDefaultZone();
	private long currentMoveStartTime;
	private long totalMovesTime = 0;
	private Dictionary<Integer, Long> playerTimes;

	private boolean mustRedo = true;
	/*
	* Private variables meant for the acceptors
	*/
	private Object acceptedValue;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
		this.logger = logger;
		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		this.myProcess = myProcess;
		this.rand = new Random();
		lock = new Semaphore(1);
		processCount = allGroupProcesses.length;
		upProcesses = processCount;
		allProcesses = allGroupProcesses;
		messagesTreeMap = new TreeMap<Integer, Object>();
		acceptedValue = null;
		lastAcceptedBallotID = -1.0;
		lastAcceptedValue = null;

		resetLeaderPromises();
		resetValuePromises();

		playerTimes = new Hashtable<Integer, Long>();

		paxosThread = new Thread(new PaxosThread());
		paxosThread.start();
	}

	synchronized private void resetLeaderPromises()
	{
		leaderPromises = new Hashtable<String, TriStateResponse>();
		for (String process : allProcesses)
		{
			leaderPromises.put(process, TriStateResponse.NORESPONSE);
		}
	}
	
	synchronized private void resetValuePromises()
	{
		valuePromises = new Hashtable<String, TriStateResponse>();
		for (String process : allProcesses)
		{
			valuePromises.put(process, TriStateResponse.NORESPONSE);
		}
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		Object[] obj = (Object[])val;
		if (playerNumber == -1){
			playerNumber = (int)obj[0];
		}
		try
		{
			boolean mustRestartPaxosProcess = true;
			while(mustRestartPaxosProcess)
			{
				currentMoveStartTime = clk.millis();
				Thread.sleep((long)(THREAD_SLEEP_MIN_MILLIS + rand.nextInt(THREAD_SLEEP_MAX_MILLIS)));
				logger.fine("Player: " + obj[0] + " is attempting to enter a paxos round with value " + obj[1]);
			
				boolean mustRestartProposePhases = true;
				while(mustRestartProposePhases)
				{
					logger.fine("Player: " + obj[0] + " entering first phase of paxos");
					resetLeaderPromises();
					//First step: Propose to be the leader
					while (!proposeToBeLeader((int)obj[0]))
					{
						resetLeaderPromises();
						Thread.sleep((long)(THREAD_SLEEP_MIN_MILLIS + rand.nextInt(THREAD_SLEEP_MAX_MILLIS)));
					}
					resetLeaderPromises();

					resetValuePromises();
					logger.fine("Player: " + obj[0] + " entering second phase of paxos");
					//Second step: Propose a value
					TriStateResponse response = proposeValue(val);
					if(response == TriStateResponse.ACCEPT)
					{
						failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
						mustRestartProposePhases = false;
					}
					else
					{
						currentProposerBallotID = currentHighestBallotID;
					}
					resetValuePromises();
				}
				
				logger.fine("Player: " + obj[0] + " entering third phase of paxos");
				//Third step confirm value
				if (lastAcceptedValue != null)
				{
					playerTimes.put(currentProcessMoveNumber,currentMoveStartTime);
					confirmValue(currentProposerBallotID, lastAcceptedValue, true);
					lastAcceptedValue = null;
					lastAcceptedBallotID = -1.0;
				}
				else
				{
					playerTimes.put(currentProcessMoveNumber,currentMoveStartTime);
					currentProcessMoveNumber++;
					confirmValue(currentProposerBallotID, val, false);
					
				}
				if (!mustRedo)
					{
						mustRestartPaxosProcess = false;
					}
			}
			Thread.sleep(20);
		}
		catch (InterruptedException e){}
	}

	synchronized private void handleProposalFromLeaderMessage(String senderProcess, int proposerPlayerID, Object proposalID)
	{
		failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);

		double proposerBallotID = (double)proposalID;
		if (proposerBallotID > currentHighestBallotID)
		{
			Object[] acceptMsg;
			if (acceptedValue == null)
			{
				logger.fine("Accepting player: " + proposerPlayerID + " to be the leader with ballotID: " + proposerBallotID);
				acceptMsg = new Object[] { PAXOS_PHASE_PROMISE_ACCEPT, proposerPlayerID, proposerBallotID };
			}
			else 
			{
				logger.fine("Accepting player: " + proposerPlayerID + " to be the leader with ballotID: " + proposerBallotID + ". However, previous value was accepted: " + acceptedValue);
				acceptMsg = new Object[] { PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE, proposerPlayerID, currentHighestBallotID,  acceptedValue };
			}

			currentHighestBallotID = proposerBallotID;
			gcl.sendMsg(acceptMsg, senderProcess);
		}
		else
		{
			logger.fine("Denying player: " + proposerPlayerID + " to be the leader with ballotID: " + proposerBallotID + ". current highest ballotID: " + currentHighestBallotID);
			Object[] denyMsg = new Object[] { PAXOS_PHASE_PROMISE_DENY, myProcess, currentHighestBallotID };
			gcl.sendMsg(denyMsg, senderProcess);
		}

		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
	}

	synchronized private void handlePromiseAcceptMessage(String senderProcess)
	{
		synchronized(leaderPromises)
		{
			try
			{
				leaderPromises.remove(senderProcess);
				leaderPromises.put(senderProcess, TriStateResponse.ACCEPT);
			}
			catch (Exception e)
			{}
		}
	}

	synchronized private void handlePromiseAcceptWithPreviousValue(String senderProcess, Object previousValueBallotID, Object previousValue)
	{
		if (((lastAcceptedBallotID >= 0 && lastAcceptedBallotID < (double)previousValueBallotID)) || lastAcceptedValue == null)
		{
			lastAcceptedBallotID = (double)previousValueBallotID;
			lastAcceptedValue = previousValue;
		}

		try
		{
			leaderPromises.remove(senderProcess);
			leaderPromises.put(senderProcess, TriStateResponse.ACCEPT);
		}
		catch (Exception e)
		{}
	}

	synchronized private void handlePromiseDenyMessage(String senderProcess, Object highestBallotID)
	{
		try
		{
			leaderPromises.remove(senderProcess);
			leaderPromises.put(senderProcess, TriStateResponse.DENY);
			currentHighestBallotID = (double)highestBallotID;
		}
		catch (Exception e){}
	}

	synchronized private void handleConfirmValue(int proposerMessageCount, Object message)
	{
		Object[] obj = (Object[]) message;
		logger.fine("Adding message to tree map: " + "{ proposerMessageCount: " + proposerMessageCount + ", " + "{ " + obj[0] + ", " + obj[1] + " } }"); 

		try
		{
			lock.acquire();
			if (messagesTreeMap.get(proposerMessageCount) == null)
			{
				messagesTreeMap.put(proposerMessageCount, message);
				globalMessageCount = ++proposerMessageCount;
			}
			else
			{
				int lastMessageKey = messagesTreeMap.lastKey() + 1;
				messagesTreeMap.put(lastMessageKey, message);
				globalMessageCount = lastMessageKey ++;
			}
			lock.release();
		}
		catch (Exception e){}

		acceptedValue = null;
	}

	synchronized private void handleProposeValueFromLeaderMessage(String senderProcess, Object ballotID, Object value)
	{
		Object[] msg;
		if ((double)ballotID >= currentHighestBallotID)
		{
			acceptedValue = value;
			msg = new Object[] { PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK };
		}
		else 
		{
			msg = new Object[] { PAXOS_PHASE_PROPOSE_VALUE_DENYACK, ballotID };	
		}
		gcl.sendMsg(msg, senderProcess);
	}

	synchronized private void handleProposeValueAcceptAckMessage(String senderProcess)
	{
		try
		{
			valuePromises.remove(senderProcess);
			valuePromises.put(senderProcess, TriStateResponse.ACCEPT);
		}
		catch (Exception e){}
	}

	synchronized private void handleProposeValueDenyAckMessage(String senderProcess, Object highestBallotID)
	{
		try
		{
			valuePromises.remove(senderProcess);
			valuePromises.put(senderProcess, TriStateResponse.DENY);
			currentHighestBallotID = (double)highestBallotID;
		}
		catch (Exception e){}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		Map.Entry message = messagesTreeMap.firstEntry();
		Object[] returnObj = null;
		
		while (message == null || (int)message.getKey() > processMessageCount)
		{
			if(!shouldContinue){
				break;
			}

			try 
			{
				Thread.sleep(THREAD_POLLING_LOOP_SLEEP);
				logger.fine("Inside polling loop");
				message = messagesTreeMap.firstEntry();
			}
			catch (Exception e) {}
		}
			
		if (message != null)
		{
			messagesTreeMap.remove((int)message.getKey());
			returnObj = (Object[])message.getValue();
			if (returnObj[0] instanceof String)
			{
				logger.fine("Retrieving termination message: " + (String)returnObj[0]);
			}
			else 
			{
				logger.fine("returning message: " + (Integer)returnObj[0] + ", " + (Character)returnObj[1]);
			}
		}

		if (message == null) {
			return null;
		}
		if (!((int)message.getKey() == Integer.MAX_VALUE))
			processMessageCount++;
		
		if ((int)((Object[])message.getValue())[0] == playerNumber) {
			totalMovesTime += clk.millis() - playerTimes.get(currentAcceptedMoveNumber);
			currentAcceptedMoveNumber++;
		}

		return returnObj;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		try
		{
			paxosThread.join(THREAD_TERMINATION_SLEEP * processCount);
			if (currentAcceptedMoveNumber != 0)
				logger.fine("----- Average move time ------ " + totalMovesTime / currentAcceptedMoveNumber);
		}
		catch (InterruptedException e) {
			// shouldContinue = false;
			// gcl.shutdownGCL();
			// return;
		}
		shouldContinue = false;
		gcl.shutdownGCL();
	}

	private boolean proposeToBeLeader(int processID)
	{
		currentProposerBallotID += 0.1;
		
		logger.fine("Player: " + processID + " proposing to be a leader with ballotID: " + currentProposerBallotID);
		Object[] obj = new Object[] { PAXOS_PHASE_PROPOSE_LEADER, processID, currentProposerBallotID };
		gcl.broadcastMsg(obj);

		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

		TriStateResponse response = hasMajority(true);
		int loopCount = 0;
		while (response == TriStateResponse.NORESPONSE)
		{	
			if (loopCount == 20)
				return false;
			logger.fine("Process: " + processID + " inside no response loop to be leader.");
			 try
			 {
			 	Thread.sleep(5);
			 }
			 catch (InterruptedException ie) {}
			response = hasMajority(true);
			loopCount++;
		}
		if (response == TriStateResponse.DENY)
		{
			currentProposerBallotID = currentHighestBallotID;
			return false;
		}
		else
		{
			failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
			return true;
		}
	}

	private TriStateResponse proposeValue(Object val)
	{
		Object[] obj;
		if (lastAcceptedValue != null)
		{
			obj = new Object[] { PAXOS_PHASE_PROPOSE_VALUE, currentProposerBallotID, lastAcceptedValue };
		}
		else
		{
			obj = new Object[] { PAXOS_PHASE_PROPOSE_VALUE, currentProposerBallotID, val };
		}

		gcl.broadcastMsg(obj);

		TriStateResponse response = hasMajority(false);
		int loopCount = 0;
		while (response == TriStateResponse.NORESPONSE)
		{	
			if (loopCount == 25)
				return TriStateResponse.DENY;
			 try
			 {
			 	Thread.sleep(5);
			 }
			 catch (InterruptedException ie) {}
			response = hasMajority(false);
			loopCount++;
		}

		return response;
	}

	private void confirmValue(double ballotID, Object val, boolean prevValue)
	{
		try
		{
			lock.acquire();
			if (messagesTreeMap.get(globalMessageCount) != null)
			{
				mustRedo = true;
				lock.release();
				return;
			}
			else
			{
				mustRedo = prevValue;
			}
			Object[] obj = new Object[] { PAXOS_PHASE_CONFIRM_VALUE, globalMessageCount, val };
			gcl.broadcastMsg(obj);
			
			lock.release();
		}
		catch (Exception e) {}
	}

	private TriStateResponse hasMajority(boolean isLeaderPhase)
	{
		int acceptCount = 0;
		int denyCount = 0;
		
		Dictionary<String, TriStateResponse> dictionary;
		if (isLeaderPhase)
		{
			dictionary = leaderPromises;
		}
		else
		{
			dictionary = valuePromises;
		}

		for (String process : allProcesses)
		{
			if (dictionary.get(process) == TriStateResponse.NORESPONSE)
			{
				continue;
			}
			else if (dictionary.get(process) == TriStateResponse.ACCEPT)
			{
				acceptCount ++;
				if (((double)acceptCount / (double)(processCount)) > 0.5)
					return TriStateResponse.ACCEPT;
			}
			else if (dictionary.get(process) == TriStateResponse.DENY)
			{
				denyCount ++;
				if (((double)denyCount / (double)(processCount)) > 0.5)
					return TriStateResponse.DENY;
			}
		}
		return TriStateResponse.NORESPONSE;
	}

	private enum TriStateResponse
	{
		NORESPONSE, ACCEPT, DENY
	}

	private class PaxosThread implements Runnable
	{
		public void run()
		{
			while (shouldContinue)
			{
				try
				{
					GCMessage gcmsg = gcl.readGCMessage();
					Object[] obj = (Object[])gcmsg.val;

					if (obj[0].equals(PAXOS_PHASE_PROPOSE_LEADER))
					{
						logger.fine("Received propose to be leader message from player: " + obj[1]);
						handleProposalFromLeaderMessage(gcmsg.senderProcess, (int)obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_ACCEPT))
					{
						logger.fine("Received promise accept message from player: " + obj[1]);
						handlePromiseAcceptMessage(gcmsg.senderProcess);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE))
					{
						logger.fine("Received promise accept with previous value message from player: " + obj[1]);
						handlePromiseAcceptWithPreviousValue(gcmsg.senderProcess, obj[2], obj[3]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_DENY))
					{
						logger.fine("Received promise deny message from player: " + obj[1]);
						handlePromiseDenyMessage(gcmsg.senderProcess, obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE))
					{
						
						Object[] o = (Object[])obj[2];
						logger.fine("Recevived propose value from process: " + o[0]);
						handleProposeValueFromLeaderMessage(gcmsg.senderProcess, obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK))
					{
						logger.fine("Recevived accept propose value from process: " + gcmsg.senderProcess);
						handleProposeValueAcceptAckMessage(gcmsg.senderProcess);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_DENYACK))
					{
						logger.fine("Recevived deny propose value from process: " + gcmsg.senderProcess);
						handleProposeValueDenyAckMessage(gcmsg.senderProcess, obj[1]);
					}
					else if (obj[0].equals(PAXOS_PHASE_CONFIRM_VALUE))
					{
						logger.fine("Recevived confirm value from process: " + gcmsg.senderProcess);
						handleConfirmValue((int)obj[1], obj[2]);
					}
				}
				catch (InterruptedException ie) 
				{ 
					
				}
			}
		}
	}
}

