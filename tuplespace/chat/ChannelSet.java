package chat;

import java.util.HashMap;
import java.util.Set;

class ChannelSet {
	HashMap<String, Integer> channels;
	
	public ChannelSet() {
		channels = new HashMap<String, Integer>();
	}
	
	public boolean add(String ch, int rows) {
		
		if (channels.containsKey(ch)) {
			if (channels.get(ch).intValue() != rows) {
				System.err.println("OOps. " + ch + " has different rows.");
			}
			return false;
		} else {
			channels.put(ch, rows);
			return true;
		}
	}

	public void merge(ChannelSet cSet) {
		Set<String> chs = cSet.channels.keySet();
		for (String ch : chs) {
			if (channels.containsKey(ch)) {
				if (!channels.get(ch).equals(cSet.channels.get(ch)))
					System.err.println("OOps. " + ch + " has different rows.");
			} else {
				channels.put(ch, cSet.channels.get(ch));
			}
		}
	}
	
	public String toString() {
		String s = "";
		Set<String> chs = channels.keySet();
		for (String ch : chs) {
			s += ch + ":" + channels.get(ch).toString() + ", ";
		}
		return s;
	}
	
	public void fromString(String s) {
		channels.clear();
		
		String[] items = s.split(", ");
		for (String item : items) {
			int loc = item.indexOf(':');
			channels.put(item.substring(0, loc), Integer.parseInt(item.substring(loc + 1)));
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
		Integer rows = channels.get(channel);
		if (rows == null) return 0;
		return rows.intValue();
	}
}
