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

	private double currentBallotID = 0.0;
	private static final long THREAD_SLEEP_MILLIS = 200;
	private static final String PAXOS_PHASE_PROPOSE_LEADER = "proposeleader";
	private static final String PAXOS_PHASE_PROPOSE_VALUE = "proposevalue";
	private static final String PAXOS_PHASE_PROMISE_ACCEPT = "promiseaccept";
	private static final String PAXOS_PHASE_PROMISE_DENY = "promisedeny";
	private static final String PAXOS_PHASE_CONFIRM_VALUE = "confirmvalue";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK = "acceptack";
	private static final String PAXOS_PHASE_PROPOSE_VALUE_DENYACK = "denyack";
	
	private int processCount;
	private Dictionary<String, TriStateResponse> promises;
	private String[] allProcesses;
	private Queue<Object> messagesQueue;
	private Thread paxosThread;
	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		processCount = allGroupProcesses.length;
		allProcesses = allGroupProcesses;
		messagesQueue = new LinkedList<Object>();
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
		//First step: Propose to be the leader
		while (!propose())
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
		//TODO

		//Third step confirm value
		confirmValue(val);
	}

	private void handleProposeLeaderMessage(String senderProcess, Object proposalID)
	{
		double proposerBallotID = (double)proposalID;
		if (proposerBallotID > currentBallotID)
		{
			currentBallotID = proposerBallotID;
			Object[] acceptMsg = new Object[] { PAXOS_PHASE_PROMISE_ACCEPT, currentBallotID };
			gcl.sendMsg(acceptMsg, senderProcess);
		}
		else
		{
			Object[] denyMsg = new Object[] { PAXOS_PHASE_PROMISE_DENY, currentBallotID };
			gcl.sendMsg(denyMsg, senderProcess);
		}
	}

	private void handlePromiseAcceptMessage(String senderProcess)
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

	private void handlePromiseDenyMessage(String senderProcess)
	{
		synchronized(promises)
		{
			try
			{
				promises.remove(senderProcess);
				promises.put(senderProcess, TriStateResponse.DENY);
			}
			catch (Exception e){}
		}
	}

	private void handleConfirmValue(Object message)
	{
		messagesQueue.add(message);
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

	private boolean propose()
	{
		currentBallotID += 0.1;
		Object[] obj = new Object[] { PAXOS_PHASE_PROPOSE_LEADER, currentBallotID };
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
		return response == TriStateResponse.ACCEPT;
	}

	private void confirmValue(Object val)
	{
		Object[] obj = new Object[] {PAXOS_PHASE_CONFIRM_VALUE, val};
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
					handleProposeLeaderMessage(gcmsg.senderProcess, obj[1]);
				}
				else if (obj[0].equals(PAXOS_PHASE_PROMISE_ACCEPT))
				{
					handlePromiseAcceptMessage(gcmsg.senderProcess);
				}
				else if (obj[0].equals(PAXOS_PHASE_PROMISE_DENY))
				{
					handlePromiseDenyMessage(gcmsg.senderProcess);
				}
				else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE))
				{
					//TODO: Phase 2.
				}
				else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_ACCEPTACK))
				{
					//TODO: Phase 2.
				}
				else if (obj[0].equals(PAXOS_PHASE_PROPOSE_VALUE_DENYACK))
				{
					//TODO: Phase 2.
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

