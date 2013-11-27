package tuplespaces;

import java.util.ArrayList;

/*
 * This implementation works for the situations where the intersection of any two tuple-sets 
 * that match corresponding patterns is empty.  
 */
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
		
		synchronized(wp){
			while ((tp = search(pattern)) == null) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		
			synchronized (space) {
				space.remove(tp);
			}
		}

		return tp;
	}

	public String[] read(String... pattern) {
		String[] tp;
		String[] wp = addWaiting(pattern);

		synchronized(wp){
			while ((tp = search(pattern)) == null) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
		return tp;
	}

	public void put(String... tuple) {
		synchronized (space) {
			space.add(tuple.clone());
		}
		
		String[] wp = notifyWaiting(tuple);
		if(wp != null){
			synchronized(wp){
				wp.notify();
			}
		}
	}
	
	private String[] search(String... pattern) {
		synchronized (space) {
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
		}
		
		return null;
	}
	
	private String[] addWaiting(String... pattern) {
		String[] wp = null;
		
		synchronized (waiting) {
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
		}
		
		return wp;
	}
	
	private String[] notifyWaiting(String... tuple) {
		synchronized (waiting) {
			for (String[] w : waiting) {
				if (w.length != tuple.length) continue;
				boolean found = true;
				for (int i = 0; i < w.length; i++) {
					if (w[i] != null && w[i] != tuple[i]) {
						found = false;
						break;
					}
				}
				if (found) return w;
			}
		}
		
		return null;
	}
}
