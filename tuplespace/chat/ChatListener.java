package chat;

import tuplespaces.TupleSpace;

public class ChatListener {

	TupleSpace ts;
	String channel;
	int rows;
	int nextRead;

	public ChatListener(TupleSpace ts, String channel, int rows, int nextRead) {
		this.ts = ts;
		this.channel = channel;
		this.rows = rows;
		this.nextRead = nextRead;
	}
	
	public String getNextMessage() {
		String nr = Integer.toString(nextRead);

		String[] tuple;
		// get reading signal
		tuple = ts.get(channel, ChatServer.SIGNALS, nr, null);
		int signals = Integer.parseInt(tuple[3]);
		
		// read message
		tuple = ts.read(channel, ChatServer.MESSAGE, nr, null);
		
		// signal chat servers
		ts.put(channel, ChatServer.SIGNALS, nr, Integer.toString(signals - 1));		
		
		nextRead++;
		return tuple[3];
	}

	public void closeConnection() {
		String[] tuple;
		
		// get latest readable position
		tuple = ts.get(channel, ChatServer.CONNECTIONS, null, null);
		int ccNum = Integer.parseInt(tuple[2]);
		int lrInt = Integer.parseInt(tuple[3]);
		
		// consume the not-yet-reading signals for this client
		while (nextRead <= lrInt) {
			getNextMessage();
		}
		
		// update number of client connections
		ts.put(channel, ChatServer.CONNECTIONS, Integer.toString(ccNum - 1), tuple[3]);
	}
}
