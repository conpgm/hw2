package tuplespaces;

import java.util.ArrayList;

public class LocalTupleSpace implements TupleSpace {
	
	ArrayList<String[]> space = new ArrayList<String[]>();
	
	public synchronized String[] get(String... pattern) {
		String[] tp;
		while ((tp = search(pattern)) == null) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
		space.remove(tp);
		return tp;
	}

	public synchronized String[] read(String... pattern) {
		String[] tp;
		while ((tp = search(pattern)) == null) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
		return tp;
	}

	public synchronized void put(String... tuple) {	
		space.add(tuple.clone());
		notifyAll();
	}
	
	private String[] search(String... pattern) {
		for (String[] tp : space) {
			if (tp.length == pattern.length) {
				boolean found = true;
				for (int i = 0; i < pattern.length; i++) {
					if (pattern[i] != null && !pattern[i].equals(tp[i])) {
						found = false;
						break;
					}
				}
				if (found) return tp;
			}
		}
		return null;
	}
}
