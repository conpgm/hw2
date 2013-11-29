package chat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import tuplespaces.TupleSpace;

public class ChatServer {
	
	public static final String 	CHANNELLIST 	= "chl";
	public static final String 	LISTENERSET 	= "lns";
	public static final String 	NEXTWRITE 		= "nxw";
	public static final String 	ACK 			= "ack";
	public static final String 	READWAIT 		= "rwt";
	public static final String 	READSIGNAL 		= "rsg";
	public static final String 	MESSAGE 		= "msg";
	
	TupleSpace ts;
	
	ChannelSet chSet = new ChannelSet();
	HashMap<String, IDSet> ackMap = new HashMap<String, IDSet>();
	HashMap<String, IDSet> listenerMap = new HashMap<String, IDSet>();
	
	public ChatServer(TupleSpace t, int rows, String[] channelNames) {
		ts = t;
		
		String[] tuple = ts.get(CHANNELLIST, null);
		chSet.fromString(tuple[1]);
		
		for (String ch : channelNames) {
			if (chSet.add(ch, rows)) {
				ts.put(ch, LISTENERSET, "");
				ts.put(ch, NEXTWRITE, "0");
				for (int i = 0; i < rows; i++)
					ts.put(ch, READWAIT, Integer.toString(i), "0");
			}
		}
		ts.put(CHANNELLIST, chSet.toString());
		
		System.out.println("[S] chlist " + chSet.toString());
		
		for (String ch : chSet.getChannels()) {
			ackMap.put(ch, new IDSet());
			listenerMap.put(ch, new IDSet());
		}
		
		System.out.println("[S] a server created.");
	}

	public ChatServer(TupleSpace t) {
		ts = t;
		String[] tuple = ts.read(CHANNELLIST, null);
		chSet.fromString(tuple[1]);
	}

	public String[] getChannels() {
		String[] tuple = ts.get(CHANNELLIST, null);
		ChannelSet set = new ChannelSet();
		set.fromString(tuple[1]);
		
		ArrayList<String> merged = chSet.merge(set);
		for (String ch : merged) {
			ackMap.put(ch, new IDSet());
			listenerMap.put(ch, new IDSet());
		}
		
		ts.put(CHANNELLIST, chSet.toString());
		
		return chSet.getChannels();
	}
	
	public void writeMessage(String channel, String message) {
		
		String[] tuple; 
		int rows = chSet.getRows(channel);
		
		// disable other chat servers
		tuple = ts.get(channel, NEXTWRITE, null);
		int wPos = Integer.parseInt(tuple[2]);
		String qPos = Integer.toString(wPos % rows);
		
		if(wPos < rows) {
			System.out.println("[S] " + channel + ": write directly " + wPos);
			ts.put(channel, MESSAGE, Integer.toString(wPos), message);
		} else {
			// before overwriting message, we wait until that message has 
			// been received by all listeners
			IDSet listeners = listenerMap.get(channel);
			IDSet acks = ackMap.get(channel);
			acks.clear();
			
			tuple = ts.read(channel, LISTENERSET, null);
			listeners.fromString(tuple[2]);
			
			System.out.println("[S] " + channel + ": lsnset " + listeners.toString());
			
			while (!listeners.isEmpty() && !listeners.containedBy(acks)){
				
				System.out.println("[S] " + channel + ": wait for writing " + qPos + " " + wPos);
				
				tuple = ts.get(channel, ACK, qPos, null);
				acks.add(tuple[3]);
				
				System.out.println("[S] " + channel + ": ackset " + acks.toString());
				
				tuple = ts.read(channel, LISTENERSET, null);
				listeners.fromString(tuple[2]);
			//	System.out.println("[S] " + channel + ": lsnset " + listeners.toString());
			}
			
			System.out.println("[S] " + channel + ": ready for writing " + qPos + " " + wPos);
			
			// overwriting the message
			ts.get(channel, MESSAGE, qPos, null);
			ts.put(channel, MESSAGE, qPos, message);
			
			System.out.println("[S] " + channel + ": after writing " + qPos + " " + wPos);
		}
		
		// enable other chat servers
		ts.put(channel, NEXTWRITE, Integer.toString(wPos + 1));
		
		// signal waiting listeners if any
		tuple = ts.get(channel, READWAIT, qPos, null);
		for (int i = 0; i < Integer.parseInt(tuple[3]); i++) {
			ts.put(channel, READSIGNAL, qPos);
		}
		ts.put(channel, READWAIT, qPos, "0");
	}

	public ChatListener openConnection(String channel) {
		String[] tuple;
		tuple = ts.get(channel, LISTENERSET, null);
		IDSet listeners = new IDSet();
		listeners.fromString(tuple[2]);
		
		System.out.println("[S] " + channel + ": before create listener");
		System.out.println("[S] " + channel + ": lsnset " + listeners.toString());
		
		tuple = ts.read(channel, NEXTWRITE, null);
		int rPos = 0;
		int rows = chSet.getRows(channel);
		int wPos = Integer.parseInt(tuple[2]);
		
		if (wPos >= rows)
			rPos = wPos - rows;

		ChatListener chatListener = new ChatListener(ts, channel, rows, rPos);
		UUID id = UUID.randomUUID();
		chatListener.setID(id.toString());
		
		listeners.add(id.toString());
		
		System.out.println("[S] " + channel + ": listener created with " + id + " " + rows + " " + rPos);

		ts.put(channel, LISTENERSET, listeners.toString());
		
		System.out.println("[S] " + channel + ": after create listener");
		
		return chatListener;
	}
}
