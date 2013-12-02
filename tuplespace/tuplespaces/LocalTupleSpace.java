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
	
//	class PatternStat {
//		String[] tuple;
//		Integer ref;
//		
//		PatternStat (String[] tuple, Integer ref) {
//			this.tuple = tuple;
//			this.ref = ref;
//		}
//	}
		
	// store all tuples
	ArrayList<String[]> space;
	// store all patterns that are used as synchronized objects
	HashMap<String[], Integer> waiting;
	
	public LocalTupleSpace () {
		space = new ArrayList<String[]>();
		waiting = new HashMap<String[], Integer>();
		
		space.add(new String[]{"chs", ""});
	}

	public String[] get(String... pattern) {
		String[] wp = addWaiting(pattern);
		synchronized(wp){
			String[] tp;
			while ((tp = searchTuple(pattern, true)) == null) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
			
			removeWaiting(wp);
			
			return tp;
		}
	}

	public String[] read(String... pattern) {
		String[] wp = addWaiting(pattern);
		synchronized(wp){
			String[] tp;
			while ((tp = searchTuple(pattern, false)) == null) {
				try {
					wp.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
			
			removeWaiting(wp);
			wp.notify();
			
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
		String[] wp = searchWaiting(tp);
		
		if(wp != null){
			synchronized(wp){
				wp.notify();
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
	
//	private void removeTuple(String[] tuple) {
//		synchronized (space) {
//			space.remove(tuple);
//		}
//	}
	
	/* 
	 * search tuple that matches pattern in tuple space and remove the matched 
	 * tuple if toRemove is true.
	 * We have to make sure that search and remove operations form an atomic 
	 * operation. Thus, we put remove here.
	 */
	private String[] searchTuple(String[] pattern, boolean toRemove) {
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
		String[] wp = pattern.clone();
		synchronized (waiting) {
			if (waiting.containsKey(wp)) {
				waiting.put(wp, new Integer(waiting.get(wp).intValue() + 1));
			} else {
				waiting.put(wp, new Integer(1));
			}
		}
		return wp;
	}
	
	private void removeWaiting(String[] wp) {
		synchronized (waiting) {
			if (waiting.containsKey(wp)) {
				waiting.put(wp, new Integer(waiting.get(wp).intValue() - 1));
			} else {
				System.err.println("Oops. Pattern doesn't exits. " + Arrays.toString(wp));
			}
		}
	}
	
	/*
	 * Find the pattern that matches tuple in waiting, return null if no
	 * one exists. Then, The caller of this function should invoke notify()
	 * if pattern exists. 
	 */
	private String[] searchWaiting(String[] tuple) {
		synchronized (waiting) {
			Set<String[]> wpSet = waiting.keySet();
			for (String[] wp : wpSet) {
				if (waiting.get(wp).intValue() == 0 || wp.length != tuple.length) continue;
				boolean found = true;
				for (int i = 0; i < wp.length; i++) {
					if (wp[i] != null && !wp[i].equals(tuple[i])) {
						found = false;
						break;
					}
				}
				if (found) return wp;
			}
		}
			
		return null;
	}
	
	private void printWaiting() {
		synchronized (waiting) {
			System.out.println("\nPrint Waiting ...");
			Set<String[]> wpSet = waiting.keySet();
			for (String[] wp : wpSet) {
				System.out.println(Arrays.toString(wp) + " => " + waiting.get(wp));
			}
			System.out.println("Print Waiting Done.\n");
		}
	}
	
	private void printSpace() {
		synchronized (space) {
			System.out.println("\nPrint Space ...");
			for (String[] tuple : space) {
				System.out.println(Arrays.toString(tuple));
			}
			System.out.println("Print Space Done.\n");
		}
	}
}
