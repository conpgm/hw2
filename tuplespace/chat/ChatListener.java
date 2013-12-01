package chat;

import tuplespaces.TupleSpace;

public class ChatListener {

	TupleSpace ts;
	String channel;
	int rows;
	int rPos;
	String id;

	public ChatListener(TupleSpace ts, String channel, int rows, int rPos, String id) {
		this.ts = ts;
		this.channel = channel;
		this.rows = rows;
		this.rPos = rPos;
		this.id = id;
	}
	
	public String getNextMessage() {
		String qPos = Integer.toString(rPos % rows);
		
//		System.out.println("[L] " + channel + ": before reading " + qPos + " " + rPos);
	
		// try to get signal
		String[] tuple;
		ts.get(channel, ChatServer.SIGNALREAD, qPos, id);
		
		// read message after getting signal
		tuple = ts.read(channel, ChatServer.MESSAGE, qPos, null);
		
		// signal chat servers that reading is done
		ts.put(channel, ChatServer.SIGNALWRITE, qPos, id);
		
//		System.out.println("[L] " + channel +": after reading " + rPos);
		
		rPos++;
		return tuple[3];
	}

	public void closeConnection() {
		
		System.out.println("[L] " + channel + ": before closeConnection");
		
		String[] tuple;
		tuple = ts.get(channel, ChatServer.LISTENERSET, null);
		IDSet listeners = new IDSet();
		listeners.fromString(tuple[2]);
		
		// update listeners list
		listeners.remove(id);
		ts.put(channel, ChatServer.LISTENERSET, listeners.toString());
		
		// just signal chat server in case it is waiting
		ts.put(channel, ChatServer.SIGNALWRITE, Integer.toString(rPos % rows), id);
		
//		System.out.println("[L] " + channel + ": after closeConnection");
	}
}
