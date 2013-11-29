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
		
		System.out.println("[L] " + channel + ": before reading " + qPos + " " + rPos);
	
		// try to get signal
		String[] tuple;
		tuple = ts.get(channel, ChatServer.SIGNALREAD, qPos, id);
		
		// read after getting signal
		tuple = ts.read(channel, ChatServer.MESSAGE, qPos, null);
		
		// signal chat servers that reading is done
		ts.put(channel, ChatServer.SIGNALWRITE, qPos, id);
		
		rPos++;
		
		System.out.println("[L] " + channel +": after reading " + rPos);
		
		return tuple[3];
	}

	public void closeConnection() {
		
		System.out.println("[L] " + channel + ": before closeConnection");
		
		String[] tuple;
		tuple = ts.get(channel, ChatServer.NEXTWRITE, null, null);
		int wPos = Integer.parseInt(tuple[2]);
		IDSet listeners = new IDSet();
		listeners.fromString(tuple[3]);
		
		// consume all the signals that have not been consumed
		for (int i = rPos; i < wPos; i++) {
			tuple = ts.get(channel, ChatServer.SIGNALREAD, Integer.toString(i % rows), id);
		}
		
		// update listeners list
		listeners.remove(id);
		ts.put(channel, ChatServer.NEXTWRITE, Integer.toString(wPos), listeners.toString());
		
		System.out.println("[L] " + channel + ": after closeConnection");
	}
}
