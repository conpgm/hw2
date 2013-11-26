package chat;

import tuplespaces.TupleSpace;

public class ChatListener {

	TupleSpace ts;
	String channel;
	int rows;
	int rPos;
	int id;

	public ChatListener(TupleSpace ts, String channel, int rows, int rPos) {
		this.ts = ts;
		this.channel = channel;
		this.rows = rows;
		this.rPos = rPos;
		id = -1;
	}
	
	public void setID(int id) {
		this.id = id;
	}
	
	public String getNextMessage() {
		String[] tuple;
		tuple = ts.read(channel, ChatServer.NEXTWRITE, null);
		
		int wPos = Integer.parseInt(tuple[2]);
		
		if (rPos == wPos) {
			tuple = ts.get(channel, ChatServer.READWAIT, null);
			ts.put(channel, ChatServer.READWAIT, Integer.toString(Integer.parseInt(tuple[2]) + 1));
			ts.get(channel, ChatServer.READSIGNAL);
		}
		
		tuple = ts.read(channel, ChatServer.MESSAGE, Integer.toString(rPos % rows), null);
		
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
