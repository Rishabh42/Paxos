package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;	import java.io.*;
import java.net.UnknownHostException;
import java.util.logging.*;
import java.util.Dictionary;
import java.util.Hashtable;
import comp512.utils.*;	
import java.util.LinkedList;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;

import java.util.logging.*;

// Any other imports that you may need.	// Any other imports that you may need.
import java.io.*;	
import java.util.logging.*;	
import java.net.UnknownHostException;

// Any other imports that you may need.


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
	GCL gcl;
	FailCheck failCheck;
	Logger logger;
	/*
	* Static helper variables
	*/
	private static final long THREAD_SLEEP_MAX_MILLIS = 15;
	private static final String PAXOS_PHASE_PROPOSE_LEADER = "proposeleader";
	private static final String PAXOS_PHASE_PROPOSE_VALUE = "proposevalue";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_DENY = "promisedeny";
	private static final String PAXOS_PHASE_CONFIRM_VALUE = "confirmvalue";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK = "acceptack";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_DENYACK = "denyack";
	private static final String PAXOS_PHASE_PROCESS_SHUTDOWN = "processshutdown";
	private static final String PAXOS_PHASE_APPLICATION_SHUTDOWN = "applicationshutdown";
	private static final String TERMINATION_MESSAGE = "termination";
	private static final String APPLICATION_TERMINATION_MESSAGE = "apptermination";
	
	private boolean shouldContinue = true;

	/*
	* Helper variables meant for both proposers and acceptors.
	*/
	private String myProcess;
	private String[] allProcesses;
	private Thread paxosThread;
	private double currentHighestBallotID = 0.0;
	private TreeMap<Double, Object> messagesTreeMap;

	/*
	* Private variables meant for the proposers/leaders
	*/
	private double currentProposerBallotID = 0.0;
	private double lastAcceptedBallotID;
	private Object lastAcceptedValue;
	private int processCount;
	private Dictionary<String, TriStateResponse> promises;
	private Dictionary<String, Boolean> processesState;

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
		processCount = allGroupProcesses.length;
		allProcesses = allGroupProcesses;
		messagesTreeMap = new TreeMap<Double, Object>();
		acceptedValue = null;
		lastAcceptedBallotID = -1.0;
		lastAcceptedValue = null;

		resetPromises();
		initializeProcessStates();

		paxosThread = new Thread(new PaxosThread());
		paxosThread.start();
	}

	synchronized private void resetPromises()
	{
		promises = new Hashtable<String, TriStateResponse>();
		for (String process : allProcesses)
		{
			promises.put(process, TriStateResponse.NORESPONSE);
		}
	}

	synchronized private void initializeProcessStates()
	{
		processesState = new Hashtable<String, Boolean>();
		for (String process : allProcesses)
		{
			processesState.put(process, true);
		}
	}

	synchronized private boolean verifyMajorityOfProcessesAreUp()
	{
		int upCount = 0;
		for (String process : allProcesses)
		{
			if (processesState.get(process))
			{
				upCount++; 
			}
		}
		return (((double)upCount) / ((double)processCount)) > 0.5;
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		Object[] obj = (Object[])val;
		boolean mustRestartPaxosProcess = true;
		while(mustRestartPaxosProcess)
		{
			boolean mustRestartProposePhases = true;
			while(mustRestartProposePhases)
			{
				//First step: Propose to be the leader
				while (!proposeToBeLeader((int)obj[0]))
				{
					resetPromises();
					try
					{
						Thread.sleep(THREAD_SLEEP_MAX_MILLIS);
					}
					catch (InterruptedException ie) {}
				}
				resetPromises();

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
				resetPromises();
			}
			
			
			//Third step confirm value
			if (lastAcceptedValue != null)
			{
				confirmValue(currentProposerBallotID, lastAcceptedValue);
				lastAcceptedValue = null;
				lastAcceptedBallotID = -1.0;
			}
			else
			{
				confirmValue(currentProposerBallotID, val);
				mustRestartPaxosProcess = false;
			}
			
			
		}
		
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
				acceptMsg = new Object[] { PAXOS_PHASE_PROMISE_ACCEPT, proposerBallotID };
			}
			else 
			{
				acceptMsg = new Object[] { PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE, currentHighestBallotID,  acceptedValue };
			}

			currentHighestBallotID = proposerBallotID;
			gcl.sendMsg(acceptMsg, senderProcess);
		}
		else
		{
			Object[] denyMsg = new Object[] { PAXOS_PHASE_PROMISE_DENY, myProcess, currentHighestBallotID };
			gcl.sendMsg(denyMsg, senderProcess);
		}

		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
	}

	synchronized private void handlePromiseAcceptMessage(String senderProcess)
	{
		synchronized(promises)
		{
			try
			{
				promises.remove(senderProcess);
				promises.put(senderProcess, TriStateResponse.ACCEPT);
			}
			catch (Exception e)
			{}
		}
	}

	synchronized private void handlePromiseAcceptWithPreviousValue(String senderProcess, Object previousValueBallotID, Object previousValue)
	{
		if ((lastAcceptedBallotID >= 0 && lastAcceptedBallotID < (double)previousValueBallotID) || lastAcceptedValue == null)
		{
			lastAcceptedBallotID = (double)previousValueBallotID;
			lastAcceptedValue = previousValue;
		}

		try
		{
			promises.remove(senderProcess);
			promises.put(senderProcess, TriStateResponse.ACCEPT);
		}
		catch (Exception e)
		{}
	}

	synchronized private void handlePromiseDenyMessage(String senderProcess, Object highestBallotID)
	{
		try
		{
			promises.remove(senderProcess);
			promises.put(senderProcess, TriStateResponse.DENY);
			currentHighestBallotID = (double)highestBallotID;
		}
		catch (Exception e){}
	}

	synchronized private void handleConfirmValue(double ballotID, Object message)
	{
		messagesTreeMap.put(ballotID, message);
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
			promises.remove(senderProcess);
			promises.put(senderProcess, TriStateResponse.ACCEPT);
		}
		catch (Exception e){}
	}

	synchronized private void handleProposeValueDenyAckMessage(String senderProcess, Object highestBallotID)
	{
		try
		{
			promises.remove(senderProcess);
			promises.put(senderProcess, TriStateResponse.DENY);
			currentHighestBallotID = (double)highestBallotID;
		}
		catch (Exception e){}
	}

	synchronized private void handleProcessShutdown(Object process)
	{
		processesState.remove((String)process);
		processesState.put((String)process, false);
		if (((String)process).equals(myProcess))
		{
			logger.fine("received shutdown from process: " + (String)process);
			messagesTreeMap.put(Double.MAX_VALUE, new Object[] { TERMINATION_MESSAGE });
		}
		else 
		{
			
			logger.fine("received shutdown from process: " + (String)process);
			if (!verifyMajorityOfProcessesAreUp())
			{
				//end the game
				Object[] obj = new Object[] { PAXOS_PHASE_APPLICATION_SHUTDOWN, myProcess };
				gcl.broadcastMsg(obj);

				try
				{
					paxosThread.join(1000);
				}
				catch (InterruptedException e) {}
				
				shouldContinue = false;
				gcl.shutdownGCL();
			}
		}
	}

	private void handleApplicationShutdown()
	{
		messagesTreeMap.put(Double.MAX_VALUE, new Object[] { APPLICATION_TERMINATION_MESSAGE });
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		Map.Entry message = messagesTreeMap.pollFirstEntry();
		Object returnObj = null;
		
		while (message == null)
		{
			try 
			{
				Thread.sleep(THREAD_SLEEP_MAX_MILLIS);
				message = messagesTreeMap.pollFirstEntry();
			}
			catch (Exception e) {}
		}
			
		if (message != null)
		{
			returnObj = message.getValue();
			if (((Object[])returnObj)[0] instanceof String)
			{
				logger.fine("Retrieving termination message: " + (String)((Object[])returnObj)[0]);
			}
		}
		return returnObj;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		Object[] obj = new Object[] { PAXOS_PHASE_PROCESS_SHUTDOWN, myProcess };
		gcl.broadcastMsg(obj);

		try
		{
			paxosThread.join(1000);
		}
		catch (InterruptedException e) {}
		
		shouldContinue = false;
		gcl.shutdownGCL();
	}

	private boolean proposeToBeLeader(int processID)
	{
		currentProposerBallotID += 0.1;
		
		Object[] obj = new Object[] { PAXOS_PHASE_PROPOSE_LEADER, processID, currentProposerBallotID };
		gcl.broadcastMsg(obj);

		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

		TriStateResponse response = hasMajority();
		while (response == TriStateResponse.NORESPONSE)
		{	
			try
			{
				Thread.sleep(THREAD_SLEEP_MAX_MILLIS);
			}
			catch (InterruptedException ie) {}
			response = hasMajority();
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

		TriStateResponse response = hasMajority();
		while (response == TriStateResponse.NORESPONSE)
		{	
			try
			{
				Thread.sleep(THREAD_SLEEP_MAX_MILLIS);
			}
			catch (InterruptedException ie) {}
			response = hasMajority();
		}

		return response;
	}

	private void confirmValue(double ballotID, Object val)
	{
		Object[] obj = new Object[] { PAXOS_PHASE_CONFIRM_VALUE, ballotID, val };
		gcl.broadcastMsg(obj);
	}

	private TriStateResponse hasMajority()
	{
		int acceptCount = 0;
		int denyCount = 0;
		
		for (String process : allProcesses)
		{
			if (promises.get(process) == TriStateResponse.NORESPONSE || process.equals(myProcess))
			{
				continue;
			}
			else if (promises.get(process) == TriStateResponse.ACCEPT)
			{
				acceptCount ++;
				if (((double)acceptCount / (double)(processCount - 1)) > 0.5)
					return TriStateResponse.ACCEPT;
			}
			else if (promises.get(process) == TriStateResponse.DENY)
			{
				denyCount ++;
				if (((double)denyCount / (double)(processCount - 1)) > 0.5)
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
						handleProposalFromLeaderMessage(gcmsg.senderProcess, (int)obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_ACCEPT))
					{
						handlePromiseAcceptMessage(gcmsg.senderProcess);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE))
					{
						handlePromiseAcceptWithPreviousValue(gcmsg.senderProcess, obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROMISE_DENY))
					{
						handlePromiseDenyMessage(gcmsg.senderProcess, obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE))
					{
						handleProposeValueFromLeaderMessage(gcmsg.senderProcess, obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK))
					{
						handleProposeValueAcceptAckMessage(gcmsg.senderProcess);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_DENYACK))
					{
						handleProposeValueDenyAckMessage(gcmsg.senderProcess, obj[1]);
					}
					else if (obj[0].equals(PAXOS_PHASE_CONFIRM_VALUE))
					{
						handleConfirmValue((double)obj[1], obj[2]);
					}
					else if (obj[0].equals(PAXOS_PHASE_PROCESS_SHUTDOWN))
					{
						handleProcessShutdown(obj[1]);
					}
					else if (obj[0].equals(PAXOS_PHASE_APPLICATION_SHUTDOWN))
					{
						handleApplicationShutdown();
					}
				}
				catch (InterruptedException ie) 
				{ 
					
				}
			}
		}
	}
}

