package tuplespaces;

import java.util.ArrayList;

public class LocalTupleSpace implements TupleSpace {
	
	ArrayList<String[]> space;
	ArrayList<String[]> waiting;
	
	public LocalTupleSpace () {
		space = new ArrayList<String[]>();
		waiting = new ArrayList<String[]>();
	}
	
	public String[] get(String... pattern) {
		String[] tp;
		String[] wp = addWaiting(pattern);
		
		while ((tp = search(pattern)) == null) {
			synchronized (wp) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
			}
		}

		synchronized (this) {
			space.remove(tp);
		}
		
		return tp;
	}

	public String[] read(String... pattern) {
		String[] tp;
		String[] wp = addWaiting(pattern);
		
		while ((tp = search(pattern)) == null) {
			synchronized (wp) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
			}
		}
		
		return tp;
	}

	public void put(String... tuple) {	
		synchronized (this) {
			space.add(tuple.clone());
		}
		
		String[] wp = notifyWaiting(tuple);
		if (wp != null) wp.notify();
	}
	
	private synchronized String[] search(String... pattern) {
		for (String[] tp : space) {
			if (tp.length == pattern.length) {
				boolean found = true;
				for (int i = 0; i < pattern.length; i++) {
					if (pattern[i] != null && pattern[i] != tp[i]) {
						found = false;
						break;
					}
				}
				if (found) return tp;
			}
		}
		return null;
	}
	
	private synchronized String[] addWaiting(String... pattern) {
		String[] wp = null;
		for (String[] w : waiting) {
			if (w.length != pattern.length) continue;
			boolean found = true;
			for (int i = 0; i < pattern.length; i++) {
				if (pattern[i] != w[i]) {
					found = false;
					break;
				}
			}
			if (found) {
				wp = w;
				break;
			}
		}
		if (wp == null) {
			wp = pattern.clone();
			waiting.add(wp);
		}
		
		return wp;
	}
	
	private synchronized String[] notifyWaiting(String... tuple) {
		for (String[] w : waiting) {
			if (w.length != tuple.length) continue;
			boolean found = true;
			for (int i = 0; i < w.length; i++) {
				if (w[i] != tuple[i]) {
					found = false;
					break;
				}
			}
			if (found) return w;
		}
		return null;
	}
}
