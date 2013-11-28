package chat;

import java.util.ArrayList;

public class IDSet extends ArrayList<String> {
	
	private static final long serialVersionUID = 1640303019358873205L;
	
	public void fromString(String s) {
		clear();
		if (s.contains("#")) {
			String[] arr = s.split("#");
			for (int i = 0; i < arr.length; i++) {
				add(arr[i]);
			}
		}
	}
	
	public String toString() {
		String s = "";
		for (String id : this) {
			if (!id.equals("")) 
				s += id + "#";
		}
		return s;
	}
	
	public boolean containedBy(IDSet set) {
		int size = size();
		if (size == 0 || size > set.size()) return false;
		
		boolean found = false;
		for (String i : this) {
			found = false;
			for (String j : set) {
				if (j.equals(i)) {
					found = true;
					break;
				}
			}
			if (!found) return false;
		}
		
		return true;
	}
}
