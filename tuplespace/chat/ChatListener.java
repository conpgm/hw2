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
		
		System.out.println("[L] " + channel + ": reading " + nr);
	
		String[] tuple;
		// get reading signal
		tuple = ts.get(channel, ChatServer.SIGNALS, nr, null);
		int signals = Integer.parseInt(tuple[3]);
		
		// read message
		tuple = ts.read(channel, ChatServer.MESSAGE, nr, null);
		
		// signal chat servers
		ts.put(channel, ChatServer.SIGNALS, nr, Integer.toString(signals - 1));
		ts.print();
		
		System.out.println("[L] " + channel +": read done. " + nr);
		
		nextRead++;
		return tuple[3];
	}

	public void closeConnection() {
		
		System.out.println("[L] " + channel + ": closing");
		
		String[] tuple;
		tuple = ts.get(channel, ChatServer.CONNECTIONS, null, null);
		int lsNum = Integer.parseInt(tuple[2]);
		int lrInt = Integer.parseInt(tuple[3]);
		
		// consume the not-yet-reading signals for this client
		while (nextRead <= lrInt) {
			getNextMessage();
		}
		
		// update listener number
		ts.put(channel, ChatServer.CONNECTIONS, Integer.toString(lsNum - 1), tuple[3]);
		
		System.out.println("[L] " + channel + ": close done.");
	}
}
