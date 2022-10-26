package comp512st.paxos;

// Access to the GCL layer
import java.io.*;
import java.net.UnknownHostException;
import java.util.logging.*;


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
	private static const long THREAD_SLEEP_MILLIS = 200;
	private static const String PAXOS_PHASE_PROPOSE_LEADER = "proposeleader";
	private static const String PAXOS_PHASE_PROPOSE_VALUE = "proposevalue";
	private static const String PAXOS_PHASE_CONFIRM_VALUE = "confirmvalue";
	
	private int processCount;
	private TriStateResponse[] promises;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		processCount = allGroupProcesses.length;
		promises = new TriStateResponse[processCount];
		for (int i = 0; i < processCount; i++)
		{
			promises[i] = TriStateResponse.NORESPONSE;
		}
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		Object[] obj = (Object[])val;
		int playerNum = (Integer)obj[0];
		char move = (Character)obj[1];

		//First step: Propose to be the leader
		while (!propose())
			Thread.sleep(THREAD_SLEEP_MILLIS);

		Object[] returnObj = new Object[] { PAXOS_PHASE_CONFIRM_VALUE, val };
		gcl.broadcastMsg(returnObj);
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		GCMessage gcmsg = gcl.readGCMessage();
		
		Object[] obj = (Object[])gcmsg.val;

		if (obj[0] == PAXOS_PHASE_CONFIRM_VALUE)
			return obj[1];
		else if (obj[0] == PAXOS_PHASE_PROPOSE_LEADER)
		{
			double proposerBallotID = obj[1];
			if (proposerBallotID > currentBallotID)
			{
				currentBallotID
			}
		}
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}

	
	private bool propose()
	{
		currentBallotID += 0.1;
		Object[] obj = new Object[] { PAXOS_PHASE_PROPOSE_LEADER, currentBallotID };
		gcl.broadcastMsg(obj);

		TriStateResponse response = hasMajority();
		while (response == TriStateResponse.NORESPONSE)
		{	
			Thread.sleep(THREAD_SLEEP_MILLIS);
			response = hasMajority();
		}
		return response == TriStateResponse.ACCEPT;
	}

	private TriStateResponse hasMajority()
	{
		int acceptCount = 0;
		int denyCount = 0;
		
		for (int i = 0; i < processCount; i++)
		{
			if (promises[i] == TriStateResponse.NORESPONSE)
			{
				continue;
			}
			else if (promises[i] == TriStateResponse.ACCEPT)
			{
				acceptCount ++;
				if (((double)acceptCount / (double)processCount) > 0.5)
					return TriStateResponse.ACCEPT;
			}
			else if (promises[i] == TriStateResponse.DENY)
			{
				denyCount ++;
				if (((double)denyCount / (double)processCount) > 0.5)
					return TriStateResponse.DENY;
			}
		}
		return TriStateResponse.NORESPONSE;
	}

	private enum TriStateResponse
	{
		NORESPONSE, ACCEPT, DENY
	}
}

