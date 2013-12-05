package chat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import tuplespaces.TupleSpace;

/*
 * Tuple format and meanings:
 * 
 * CHANNELSET 	["chs", "Foo:10#Bar:10#"]
 *   	There are 2 channels, which are Foo and Bar, and both of them
 *   	have 10 rows.
 *   
 * NEXTWRITE	["Foo", "nxw", "21"]
 * 		The next writing position of "Foo" channel is 21.
 * 
 * CONNECTIONS	["Foo", "con", "5", "20"]
 * 		There are 5 clients listening to "Foo" channel and the latest
 * 		position available to read is 20.
 * 
 * SIGNALS		["Foo", "sgl", "20", "4"]
 * 		The message sent to channel "Foo" in position 20 still needs 4
 * 		clients to read.
 * 
 * MESSAGE		["Foo", "msg", "20", "hello"]
 * 		The message sent to channel "Foo" in position 20 is "Hello".
 */

public class ChatServer {
	
	public static final String 	CHANNELSET 		= "chs";
	public static final String 	NEXTWRITE 		= "nxw";
	public static final String 	CONNECTIONS 	= "con";
	public static final String 	SIGNALS			= "sgl";
	public static final String 	MESSAGE 		= "msg";
	
	final TupleSpace ts;
	// the concurrency is kept by tuple space, thus no extra synchronization is
	// needed for ChannelSet
	final ChannelSet chSet = new ChannelSet();
	
	public ChatServer(TupleSpace t, int rows, String[] channelNames) {
		ts = t;
		
		// add channels to channel set
		String[] tuple = ts.get(CHANNELSET, null);
		chSet.fromString(tuple[1]);
		for (String ch : channelNames) {
			if (chSet.add(ch, rows)) {
				ts.put(ch, NEXTWRITE, "0");
				ts.put(ch, CONNECTIONS, "0", "-1");
			}
		}
		ts.put(CHANNELSET, chSet.toString());	
	}

	public ChatServer(TupleSpace t) {
		ts = t;
		
		// use previous channels from channel set
		String[] tuple = ts.read(CHANNELSET, null);
		chSet.fromString(tuple[1]);
	}

	public String[] getChannels() {
		String[] tuple = ts.get(CHANNELSET, null);
		ChannelSet set = new ChannelSet();
		set.fromString(tuple[1]);
		
		// update channel set if new channels are created
		chSet.merge(set);
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
		
		// update number of client connections
		tuple = ts.get(channel, CONNECTIONS, null, null);
		int ccNum = Integer.parseInt(tuple[2]);
		ts.put(channel, CONNECTIONS, Integer.toString(ccNum + 1), tuple[3]);
		
		// enable other chat servers
		ts.put(channel, NEXTWRITE, Integer.toString(nwInt));
		
		return new ChatListener(ts, channel, rows, nrInt);
	}
	
	
	/*
	 * Inner class ChannelSet for channel management
	 */
	private final class ChannelSet {
		
		HashMap<String, String> channels = new HashMap<String, String>();
		
		public boolean add(String ch, int rows) {
			if (channels.containsKey(ch)) {
				if (Integer.parseInt(channels.get(ch)) != rows) {
					System.err.println("OOps. " + ch + " has different rows.");
				}
				return false;
			} else {
				channels.put(ch, Integer.toString(rows));
				return true;
			}
		}

		public ArrayList<String> merge(ChannelSet set) {
			ArrayList<String> merged = new ArrayList<String> ();
			Set<String> chs = set.channels.keySet();
			for (String ch : chs) {
				if (channels.containsKey(ch)) {
					if (Integer.parseInt(channels.get(ch)) != 
						Integer.parseInt(set.channels.get(ch)))
						System.err.println("OOps. " + ch + " has different rows.");
				} else {
					channels.put(ch, set.channels.get(ch));
					merged.add(ch);
				}
			}
			return merged;
		}
		
		public String toString() {
			String s = "";
			Set<String> chs = channels.keySet();
			for (String ch : chs) {
				s += ch + ":" + channels.get(ch) + "#";
			}
			return s;
		}
		
		public void fromString(String s) {
			channels.clear();
			if (s.contains("#")) {
				String[] items = s.split("#");
				for (String item : items) {
					int loc = item.indexOf(':');
					channels.put(item.substring(0, loc), item.substring(loc + 1));
				}
			}
		}
		
		public String[] getChannels() {
			Set<String> chs = channels.keySet();
			if (chs.isEmpty()) return null;
					
			String[] ret = new String[chs.size()];
			int i = 0;
			for (String ch : chs) {
				ret[i++] = ch;
			}
			
			return ret;
		}
		
		public int getRows(String channel) {
			String rows = channels.get(channel);
			if (rows == null) return 0;
			return Integer.parseInt(rows);
		}
	}
}