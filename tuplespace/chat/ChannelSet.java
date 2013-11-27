package chat;

import java.util.HashMap;
import java.util.Set;

class ChannelSet {
	HashMap<String, String> channels;
	
	public ChannelSet() {
		channels = new HashMap<String, String>();
	}
	
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

	public void merge(ChannelSet set) {
		Set<String> chs = set.channels.keySet();
		for (String ch : chs) {
			if (channels.containsKey(ch)) {
				if (Integer.parseInt(channels.get(ch)) != Integer.parseInt(set.channels.get(ch)))
					System.err.println("OOps. " + ch + " has different rows.");
			} else {
				channels.put(ch, set.channels.get(ch));
			}
		}
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
