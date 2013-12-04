package chat;

import java.util.ArrayList;
import java.util.HashMap;

import tuplespaces.TupleSpace;

public class ChatServer {
	
	public static final String 	CHANNELSET 		= "chs";
	public static final String 	NEXTWRITE 		= "nxw";
	public static final String 	CONNECTIONS 	= "con";
	public static final String 	SIGNALS			= "sgl";
	public static final String 	MESSAGE 		= "msg";
	
	TupleSpace ts;
	
	ChannelSet chSet = new ChannelSet();
	HashMap<String, IDSet> sigwrtMap = new HashMap<String, IDSet>();
	HashMap<String, IDSet> listenerMap = new HashMap<String, IDSet>();
	
	public ChatServer(TupleSpace t, int rows, String[] channelNames) {
		ts = t;
		
		String[] tuple = ts.get(CHANNELSET, null);
		chSet.fromString(tuple[1]);
		
		for (String ch : channelNames) {
			if (chSet.add(ch, rows)) {
				ts.put(ch, NEXTWRITE, "0");
				ts.put(ch, CONNECTIONS, "0", "-1");
			}
		}
		
		ts.put(CHANNELSET, chSet.toString());
		
		for (String ch : chSet.getChannels()) {
			sigwrtMap.put(ch, new IDSet());
			listenerMap.put(ch, new IDSet());
		}		
	}

	public ChatServer(TupleSpace t) {
		ts = t;
		String[] tuple = ts.read(CHANNELSET, null);
		chSet.fromString(tuple[1]);
	}

	public String[] getChannels() {
		String[] tuple = ts.get(CHANNELSET, null);
		ChannelSet set = new ChannelSet();
		set.fromString(tuple[1]);
		
		ArrayList<String> merged = chSet.merge(set);
		for (String ch : merged) {
			sigwrtMap.put(ch, new IDSet());
			listenerMap.put(ch, new IDSet());
		}
		
		ts.put(CHANNELSET, chSet.toString());
		
		return chSet.getChannels();
	}
	
	public void writeMessage(String channel, String message) {
		String[] tuple;

		// disable other chat servers and get writing position
		tuple = ts.get(channel, NEXTWRITE, null);
		String nw = tuple[2];
		int nwInt = Integer.parseInt(tuple[2]);
		
		int rows = chSet.getRows(channel);
		if (nwInt >= rows) {
			// wait for oldest message
			String old = Integer.toString(nwInt - rows);
			ts.get(channel, SIGNALS, old, "0");
			// reclaim oldest message
			ts.get(channel, MESSAGE, old, null);
		}
		
		// put latest message into current writing position
		ts.put(channel, MESSAGE, nw, message);
		
		// get the number of connections and signal clients that 
		// message is ready to read
		tuple = ts.get(channel, CONNECTIONS, null, null);
		ts.put(channel, SIGNALS, nw, tuple[2]);
		
		// update latest readable position
		ts.put(channel, CONNECTIONS, tuple[2], nw);
		
		// enable other chat servers
		ts.put(channel, NEXTWRITE, Integer.toString(nwInt + 1));
	}

	public ChatListener openConnection(String channel) {
		String[] tuple;
		// In order to make sure the new listener starts reading from the 
		// correct position, we have to disable other chat servers
		tuple = ts.get(channel, NEXTWRITE, null);
		int nwInt = Integer.parseInt(tuple[2]);
		
		// determine next reading position for new client
		int rows = chSet.getRows(channel);
		int nrInt = 0;
		if (nwInt > rows)
			nrInt = nwInt - rows;
		
		// signal the new client that the messages from next reading position 
		// to latest readable position (which is nwInt - 1) are ready to read
		for (int i = nrInt; i < nwInt; i++) {
			String read = Integer.toString(i);
			tuple = ts.get(channel, SIGNALS, read, null);
			int lsNum = Integer.parseInt(tuple[3]);
			ts.put(channel, SIGNALS, read, Integer.toString(lsNum + 1));
		}
		
		// update number of connections
		tuple = ts.get(channel, CONNECTIONS, null, null);
		int lsNum = Integer.parseInt(tuple[2]);
		
		ts.put(channel, CONNECTIONS, Integer.toString(lsNum + 1), tuple[3]);
		
		// enable other chat servers
		ts.put(channel, NEXTWRITE, Integer.toString(nwInt));
		
		return new ChatListener(ts, channel, rows, nrInt);
	}
}
