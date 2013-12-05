package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;

//import tuplespaces.LocalTupleSpace.Lock;
//import tuplespaces.LocalTupleSpace.Pattern;

/*
 * Tuple Space implementation. It works this way: when a tuple is put into 
 * the tuple space, only the thread that waits for that tuple is notified, 
 * thereby improving the performance of concurrency.
 */

public class LocalTupleSpace implements TupleSpace {
	
	private class Pattern {
		String[] ptn;
		
		Pattern (String[] ptn) {
			this.ptn = ptn.clone();
		}
		
		public boolean matches(String[] tuple) {
			if (ptn.length != tuple.length) return false;
			for (int i = 0; i < ptn.length; i++) {
				if (ptn[i] != null && !ptn[i].equals(tuple[i])) return false;
			}
			return true;
		}
	}
	
	// store all tuples
	ArrayList<String[]> space = new ArrayList<String[]>();
	// A map between waiting pattern and corresponding lock
	ArrayList<Pattern> waiting = new ArrayList<Pattern>();
	
	public LocalTupleSpace () {
		// for channel initialization
		space.add(new String[]{"chs", ""});
	}

	public String[] get(String... pattern) {
		
		Pattern p;
		synchronized (waiting) {
			p = new Pattern(pattern);
			waiting.add(p);
		}
		
		String[] tp;
		synchronized(p){
			while ((tp = searchTuple(p, true)) == null) {
				try {
					p.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}		
		}
		
		synchronized (waiting) {
			waiting.remove(p);
		}
		
		return tp;
	}

	public String[] read(String... pattern) {
		
		Pattern p;
		synchronized (waiting) {
			p = new Pattern(pattern);
			waiting.add(p);

		}
		
		String[] tp;
		synchronized(p){
			while ((tp = searchTuple(p, false)) == null) {
				try {
					p.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}		
		}
		
		synchronized (waiting) {
			waiting.remove(p);
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
		
		String[] tp = addTuple(tuple);
		
		synchronized (waiting) {
			for (Pattern p : waiting) {
				if (p.matches(tp)) {
					synchronized (p) {
						p.notify();
					}
				}
			}
		}
	}
	
	private String[] addTuple(String... tuple) {
		String[] tp = tuple.clone();
		synchronized (space) {
			space.add(tp);
		}
		return tp;
	}
	
	/* 
	 * search tuple that matches pattern in tuple space and remove the matched 
	 * tuple if toRemove is true.
	 * We have to make sure that search and remove operations form an atomic 
	 * operation. Thus, we put tuple-removing here.
	 */
	private String[] searchTuple(Pattern pattern, boolean toRemove) {
		synchronized (space) {
			for (String[] tp : space) {
				if (pattern.matches(tp)) {
					if (toRemove) space.remove(tp);
					return tp;
				}
			}
			return null;
		}
	}
}
