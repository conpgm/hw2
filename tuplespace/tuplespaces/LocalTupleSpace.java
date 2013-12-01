package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;

/*
 * Tuple Space implementation. It works this way: when a tuple is put into 
 * the tuple space, only the thread that waits for that tuple is notified, 
 * thereby improving the performance of concurrency.
 */
public class LocalTupleSpace implements TupleSpace {
	
	// store all tuples
	ArrayList<String[]> space;
	// store all patterns that are used as synchronized objects
	ArrayList<String[]> waiting;
	
	public LocalTupleSpace () {
		space = new ArrayList<String[]>();
		waiting = new ArrayList<String[]>();
		
		space.add(new String[]{"chs", ""});
	}

	public String[] get(String... pattern) {
		String[] tp;
		
		String[] wp = addWaiting(pattern);
		synchronized(wp){
			while ((tp = search(pattern, true)) == null) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		}

		return tp;
	}

	public String[] read(String... pattern) {
		String[] tp;
		
		String[] wp = addWaiting(pattern);
		synchronized(wp){
			while ((tp = search(pattern, false)) == null) {
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
		for (String s : tuple) {
			if (s == null) {
				System.err.println("OOps. Tuple " + 
						Arrays.toString(tuple) + " contains null.");
				return ;
			}
		}
		
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
	
	/* 
	 * search tuple that matches pattern in tuple space and remove the matched 
	 * tuple if toRemove is true.
	 * We have to make sure that search and remove operations form an atomic 
	 * operation. Thus, we put remove here.
	 */
	private String[] search(String[] pattern, boolean toRemove) {
		synchronized (space) {
			for (String[] tp : space) {
				if (tp.length == pattern.length) {
					boolean found = true;
					for (int i = 0; i < pattern.length; i++) {
						if (pattern[i] != null && !pattern[i].equals(tp[i])) {
							found = false;
							break;
						}
					}
					if (found) {
						if(toRemove) space.remove(tp);
						return tp;
					}
				}
			}
		}
		
		return null;
	}
	
	/*
	 * If there is an existed pattern that is used as a synchronized object
	 * by some thread, continue using that pattern. If not, then use the new 
	 * pattern as a synchronized object. Then, the caller of this function 
	 * might invoke wait() if necessary.
	 */
	private String[] addWaiting(String... pattern) {
		String[] wp = null;
		
		synchronized (waiting) {
			for (String[] w : waiting) {
				if (w.length != pattern.length) continue;
				boolean found = true;
				for (int i = 0; i < pattern.length; i++) {
					if (!((pattern[i] == null && w[i] == null) ||
							pattern[i] != null && pattern[i].equals(w[i]))) {
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
	
	/*
	 * Find the pattern that matches tuple in waiting, return null if no
	 * one exists. Then, The caller of this function should invoke notify()
	 * if pattern exists. 
	 */
	private String[] notifyWaiting(String... tuple) {
		synchronized (waiting) {
			for (String[] w : waiting) {
				if (w.length != tuple.length) continue;
				boolean found = true;
				for (int i = 0; i < w.length; i++) {
					if (w[i] != null && !w[i].equals(tuple[i])) {
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
