package chat;

public class IDSet {
	final int max = 100;
	int[] ids;
	int length;
	
	public IDSet() {
		ids = new int[max];
		length = 0;
	}
	
	public void fromString(String s) {
		String[] arr = s.split(",");
		for (int i = 0; i < arr.length; i++) {
			ids[i] = Integer.parseInt(arr[i]);
		}
		length = arr.length;
	}
	
	public String toString() {
		String s = "";
		for (int i = 0; i < length; i++) {
			if (ids[i] != -1)
				s += ids[i] + ", ";
		}
		return s;
	}
	
	public boolean add(int id) {
		if (length >= max) return false;
		ids[length] = id;
		length++;
		return true;
	}
	
	public boolean remove(int id) {
		boolean found = false;
		for (int i = 0; i < length; i++) {
			if (ids[i] == id) {
				ids[i] = -1;
				found = true;
				break;
			}
		}
		return found;
	}
	
	public void clear() {
		length = 0;
	}
	
	public boolean containedBy(IDSet set) {
		if (length > set.length) return false;
		
		boolean found = false;
		for (int i = 0; i < length; i++) {
			found = false;
			for (int j = 0; j < set.length; j++) {
				if (set.ids[j] == ids[i]) { 
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		return true;
	}
}
