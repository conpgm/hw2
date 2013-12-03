package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

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
		
		// new two override functions are used by HashMap class 
		@Override
		public int hashCode() {
			int sum = 0;
			for (String s : ptn) {
				if (s != null) sum += s.hashCode();
			}
			return sum;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof Pattern)) return false;
			if (obj == this) return true;
			
			Pattern other = (Pattern) obj;
			if (this.ptn.length != other.ptn.length) return false;
			for (int i = 0; i < this.ptn.length; i++) {
				if (!((this.ptn[i] == other.ptn[i]) ||
					  (this.ptn[i] != null && this.ptn[i].equals(other.ptn[i])))) 
					  return false;
			}
			return true;
		}
	}
	
	// object for synchronization between threads
	// if the lock is used, which is ref is greater than 0, there must be
	// threads that are waiting for the lock 
	private class Lock {
		int ref = 0;
	}
		
	// store all tuples
	ArrayList<String[]> space = new ArrayList<String[]>();
	// A map between waiting pattern and corresponding lock
	HashMap<Pattern, Lock> waiting = new HashMap<Pattern, Lock>();
	
	public LocalTupleSpace () {
		// for channel initialization
		space.add(new String[]{"chs", ""});
	}

	public String[] get(String... pattern) {
		// new a lock for pattern and increase ref if waiting
		Pattern p = new Pattern(pattern);
		Lock l = newLock(p);
		synchronized(l){
			String[] tp;
			while ((tp = searchTuple(p, true)) == null) {
				try {
					l.ref++;
					l.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
			return tp;
		}
	}

	public String[] read(String... pattern) {
		Pattern p = new Pattern(pattern);
		Lock l = newLock(p);
		synchronized(l){
			String[] tp;
			while ((tp = searchTuple(p, false)) == null) {
				try {
					l.ref++;
					l.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
			// notify other threads that might be waiting for read if any
			l.ref--;
			l.notify();
			return tp;
		}
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
		Lock l = getLock(tp);
		
		if(l != null){
			synchronized(l){
				l.ref--;
				l.notify();
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
	
	/*
	 * If there is an existed lock that is used by pattern p, we continue  
	 * using that lock. If not, then create a new lock for that pattern. 
	 * The caller of this function will invoke wait() if necessary.
	 */
	private Lock newLock(Pattern p) {
		Lock l;
		synchronized (waiting) {
			if (waiting.containsKey(p)) {
				l = waiting.get(p);	
			} else {
				l = new Lock();
				waiting.put(p, l);
			}
		}
		return l;
	}
	
	/*
	 * Find the used lock of some pattern that matches the tuple.
	 * Return null if no lock exists or lock is not used.
	 * The caller of this function should invoke notify() later.
	 */
	private Lock getLock(String[] tuple) {
		synchronized (waiting) {
			Set<Pattern> ps = waiting.keySet();
			for (Pattern p : ps) {
				Lock l = waiting.get(p);
				if (l.ref != 0 && p.matches(tuple)) return l;
			}
		}
		return null;
	}
}
