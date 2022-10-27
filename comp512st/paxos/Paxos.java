package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;	import java.io.*;
import java.net.UnknownHostException;
import java.util.logging.*;
import java.util.Dictionary;
import java.util.Hashtable;
import comp512.utils.*;	
import java.util.LinkedList;
import java.util.Queue;

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

	private double currentProposerBallotID = 0.0;
	private double currentHighestBallotID = 0.0;
	private static final long THREAD_SLEEP_MILLIS = 200;
	private static final String PAXOS_PHASE_PROPOSE_LEADER = "proposeleader";
	private static final String PAXOS_PHASE_PROPOSE_VALUE = "proposevalue";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT_WITH_PREVIOUS_VALUE = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_DENY = "promisedeny";
	private static final String PAXOS_PHASE_CONFIRM_VALUE = "confirmvalue";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK = "acceptack";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_DENYACK = "denyack";
	
	private int processCount;
	private Dictionary<String, TriStateResponse> promises;
	private String[] allProcesses;
	private Queue<Object> messagesQueue;
	private Thread paxosThread;
	private Object acceptedValue;
	private double lastAcceptedBallotID;
	private Object lastAcceptedValue;
	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		processCount = allGroupProcesses.length;
		allProcesses = allGroupProcesses;
		messagesQueue = new LinkedList<Object>();
		acceptedValue = null;
		lastAcceptedValue = 0;

		resetPromises();

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

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		boolean mustRestartPaxosProcess = true;
		while(mustRestartPaxosProcess)
		{
			//First step: Propose to be the leader
			while (!proposeToBeLeader())
			{
				resetPromises();
				try
				{
					Thread.sleep(THREAD_SLEEP_MILLIS);
				}
				catch (InterruptedException ie) {}
			}
			resetPromises();

			//Second step: Propose a value
			TriStateResponse response = proposeValue(val);
			if(response == TriStateResponse.ACCEPT)
			{
				mustRestartPaxosProcess = false;
			}
			else
			{
				
			currentProposerBallotID = currentHighestBallotID;
			}
		}

		//Third step confirm value
		confirmValue(val);
	}

	synchronized private void handleProposalFromLeaderMessage(String senderProcess, Object proposalID)
	{
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
			Object[] denyMsg = new Object[] { PAXOS_PHASE_PROMISE_DENY, currentHighestBallotID };
			gcl.sendMsg(denyMsg, senderProcess);
		}
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
		if (lastAcceptedBallotID < (double)previousValueBallotID || lastAcceptedValue == null)
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

	synchronized private void handleConfirmValue(Object message)
	{
		messagesQueue.add(message);
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

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		Object message = messagesQueue.peek();
		while (message == null)
		{
			try 
			{
				Thread.sleep(THREAD_SLEEP_MILLIS);
				message = messagesQueue.peek();
			}
			catch (Exception e) {}
		}
		return messagesQueue.remove();
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}

	private boolean proposeToBeLeader()
	{
		currentProposerBallotID += 0.1;
		Object[] obj = new Object[] { PAXOS_PHASE_PROPOSE_LEADER, currentProposerBallotID };
		gcl.broadcastMsg(obj);

		TriStateResponse response = hasMajority();
		while (response == TriStateResponse.NORESPONSE)
		{	
			try
			{
				Thread.sleep(THREAD_SLEEP_MILLIS);
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
			return true;
		}
	}

	private TriStateResponse proposeValue(Object val)
	{
		Object[] obj;
		if (lastAcceptedValue != null)
		{
			obj = new Object[] { PAXOS_PHASE_PROPOSE_VALUE, currentProposerBallotID, lastAcceptedValue };
			//TODO: The proposed value must still be pushed.
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
				Thread.sleep(THREAD_SLEEP_MILLIS);
			}
			catch (InterruptedException ie) {}
			response = hasMajority();
		}

		return response;
	}

	private void confirmValue(Object val)
	{
		Object[] obj = new Object[] { PAXOS_PHASE_CONFIRM_VALUE, val };
		gcl.broadcastMsg(obj);
	}

	private TriStateResponse hasMajority()
	{
		int acceptCount = 0;
		int denyCount = 0;
		
		for (String process : allProcesses)
		{
			if (promises.get(process) == TriStateResponse.NORESPONSE)
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
			try
			{
				GCMessage gcmsg = gcl.readGCMessage();
				Object[] obj = (Object[])gcmsg.val;

				if (obj[0].equals(PAXOS_PHASE_PROPOSE_LEADER))
				{
					handleProposalFromLeaderMessage(gcmsg.senderProcess, obj[1]);
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
					handlePromiseDenyMessage(gcmsg.senderProcess, obj[1]);
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
					handleConfirmValue(obj[1]);
				}
			}
			catch (InterruptedException ie) 
			{ 
				
			}
		}
	}
}

