package chat;

import tuplespaces.TupleSpace;

public class ChatListener {

	TupleSpace ts;
	String channel;
	int rows;
	int rPos;
	String id;

	public ChatListener(TupleSpace ts, String channel, int rows, int rPos) {
		this.ts = ts;
		this.channel = channel;
		this.rows = rows;
		this.rPos = rPos;
		id = "";
	}
	
	public void setID(String id) {
		this.id = id;
	}
	
	public String getNextMessage() {
		String[] tuple;
		tuple = ts.read(channel, ChatServer.NEXTWRITE, null);
		int wPos = Integer.parseInt(tuple[2]);
		
		String qPos = Integer.toString(rPos % rows);
		
		System.out.println("[L] " + channel + ": before reading " + rPos);
		
		if (rPos == wPos) {			
			tuple = ts.get(channel, ChatServer.READWAIT, qPos, null);

			System.out.println("[L] " + channel + ": wait for reading " + rPos);
			
			ts.put(channel, ChatServer.READWAIT, qPos, Integer.toString(Integer.parseInt(tuple[3]) + 1));
			ts.get(channel, ChatServer.READSIGNAL, qPos);
	
			System.out.println("[L] " + channel + ": ready for reading " + rPos);
		}
		
		tuple = ts.read(channel, ChatServer.MESSAGE, qPos, null);
		ts.put(channel, ChatServer.ACK, qPos, id);
		
		System.out.println("[L] " + channel +": after reading " + rPos);
		
		rPos++;
		return tuple[3];
	}

	public void closeConnection() {
		String[] tuple;
		tuple = ts.get(channel, ChatServer.LISTENERSET, null);
		
		IDSet listeners = new IDSet();
		listeners.fromString(tuple[2]);
		listeners.remove(id);
	
		ts.put(channel, ChatServer.LISTENERSET, listeners.toString());
	}
}
