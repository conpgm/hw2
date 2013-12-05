package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

/*
 * Tuple Space implementation. It works this way: when a tuple is put into 
 * the tuple space, only the thread that waits for that tuple is notified, 
 * thereby improving the performance of concurrency.
 * 
 * There is a Lock object attached to each Pattern object. A reference to Lock
 * object means that the Lock object is currently used by some thread that is 
 * waiting. We maintain the number of references of Lock objects and remove the
 * Lock object if the number becomes zero.
 * 
 * Note that if read is waked up by some thread it should notify other thread
 * as well because there might be a waiting get/read that wants to have the
 * same tuple that matches its own pattern. That is, a tuple put into the space
 * matches two different waiting patterns.
 * 
 * The reason why we notify other thread after get is that the same lock 
 * might be notified when two tuples are put into the space. For example, a
 * thread A is waiting ("ABC") and thread B is waiting (null) and they should
 * have two different locks. Suppose tuple ("ABC") is put into space two times 
 * it might be possible that the thread A is notified two times.
 */

public class LocalTupleSpace implements TupleSpace {

	// store all tuples
	final ArrayList<String[]> space = new ArrayList<String[]>();
	// A map between waiting pattern and corresponding lock
	final HashMap<Pattern, Lock> waiting = new HashMap<Pattern, Lock>();
	
	public LocalTupleSpace () {
		// initialize channel set
		space.add(new String[]{"chs", "null"});
	}

	public String[] get(String... pattern) {
		Pattern p = new Pattern(pattern);
		Lock l = newLock(p);
		
		String[] tp;
		synchronized(l){
			while ((tp = searchTuple(p, true)) == null) {
				try {
					l.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
		delLock(p);
		
		// avoid the same lock is gotten two or more times
		l = getLock(tp);
		if(l != null){
			synchronized(l){
				l.notify();
			}
		}
		
		return tp;
	}

	public String[] read(String... pattern) {
		Pattern p = new Pattern(pattern);
		Lock l = newLock(p);
		
		String[] tp;
		synchronized(l){
			while ((tp = searchTuple(p, false)) == null) {
				try {
					l.wait();

				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}		
		}
		
		delLock(p);
		
		// notify other threads that might be waiting if any
		l = getLock(tp);
		if(l != null){
			synchronized(l){
				l.notify();
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
		
		String[] tp = addTuple(tuple);
		
		Lock l = getLock(tp);
		if(l != null){
			synchronized(l){
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
		synchronized (waiting) {
			Lock l;
			if (waiting.containsKey(p)) {
				l = waiting.get(p);
			} else {
				l = new Lock();
				waiting.put(p, l);
			}
			l.ref++;
			return l;
		}
	}
	
	/*
	 * Delete the lock if its reference becomes zero.
	 */
	private void delLock(Pattern p) {
		synchronized (waiting) {
			Lock l = waiting.get(p);
			l.ref--;
			if (l.ref == 0) {
				waiting.remove(p);
			}
		}
	}
	
	/*
	 * Find the used lock of some pattern that matches the tuple.
	 * Return null if no lock exists.
	 * The caller of this function should invoke notify() later.
	 */
	private Lock getLock(String[] tuple) {
		synchronized (waiting) {
			Set<Pattern> ps = waiting.keySet();
			for (Pattern p : ps) {
				if (p.matches(tuple)) return waiting.get(p);
			}
			return null;
		}
	}
	
	/*
	 * Inner class Pattern and Lock
	 */
	private final class Pattern {
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
		
		// next two override functions are used by HashMap class 
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
	
	/* 
	 * object for synchronization between threads
	 * if the lock is used, then ref is greater than 0
	 */
	private final class Lock {
		int ref = 0;
	}
}
