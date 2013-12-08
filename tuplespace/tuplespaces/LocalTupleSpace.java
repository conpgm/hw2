package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/*
 * Tuple Space implementation. It provides both concurrent efficiency and 
 * searching efficiency. 
 * 
 * For concurrent efficiency, it works this way: when a tuple is put into the 
 * tuple space, only the thread that waits for that tuple is notified, thereby 
 * improving the performance of concurrency. There is a Lock object attached to 
 * each Pattern object. A reference to Lock object means that the Lock object 
 * is currently used by some thread that is waiting. We maintain the number of 
 * references of Lock objects and remove the Lock object if the number becomes 
 * zero.
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
 * 
 * For searching (pattern matching) efficiency, the algorithm works this way:
 * For a pattern like [str1, str2, null, str4], there will be 3 candidate 
 * sets (c1, c2 and c3). The length of every tuple in any of them is 4 and
 * C1 contains the tuples whose first item is str1, C2 contains the tuples 
 * whose second item is str2 and C3 contains the tuples whose fourth item 
 * is str4. Afterwards we choose one, say c2, who has the minimum size from 
 * candidate sets. Then we check if there is a tuple in c2 that also exists 
 * in c1 and c3, namely the intersection of all candidate sets. If the 
 * candidate sets are empty (in the case of all-null pattern, i.e.
 * [null, null, ..., null]), we return the tuple directly from base.
 * 
 * As we use HashSet to store the tuples in candidate sets, the average 
 * time will be O(N) where N is the number of tuples in minimal candidate set. 
 * Normally the N is quite small comparing to the number of all tuples 
 * stored in the space. The reason why we don't use Tree is that Tree might 
 * not be able to match the pattern that starts from null or has null in 
 * the middle like [null, str2, null, str4].
 */

public class LocalTupleSpace implements TupleSpace {
	
	// store all tuples
	private final TupleBase space;
	// A map between waiting pattern and corresponding lock
	private final HashMap<Pattern, Lock> waiting;
	
	public LocalTupleSpace () {
		space = new TupleBase();
		waiting = new HashMap<Pattern, Lock>();
		space.add(new Tuple("chs", ""));
	}

	public String[] get(String... pattern) {
		Pattern p = new Pattern(pattern);
		Tuple t;
		
		Lock l = newLock(p);
		synchronized(l){
			while ((t = space.search(p, true)) == null) {
				try {
					l.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}		
		}
		delLock(p);
		
		// avoid the same lock is obtained two or more times
		l = getLock(t);
		if(l != null){
			synchronized(l){
				l.notify();
			}
		}
		
		return t.getStrings();
	}

	public String[] read(String... pattern) {
		Pattern p = new Pattern(pattern);
		Tuple t;
		
		Lock l = newLock(p);
		synchronized(l){
			while ((t = space.search(p, false)) == null) {
				try {
					l.wait();
				} catch (InterruptedException e) {
					System.err.println(e.getMessage());
				}
			}
		}
		delLock(p);
		
		// notify other threads that might be waiting if any
		l = getLock(t);
		if(l != null){
			synchronized(l){
				l.notify();
			}
		}
		
		return t.getStrings();
	}

	public void put(String... tuple) {
		Tuple t = new Tuple(tuple);
		space.add(t);
		
		Lock l = getLock(t);
		if(l != null){
			synchronized(l){
				l.notify();
			}
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
	private Lock getLock(Tuple tpl) {
		synchronized (waiting) {
			Set<Pattern> ps = waiting.keySet();
			for (Pattern p : ps) {
				if (p.matches(tpl)) return waiting.get(p);
			}
		}
		return null;
	}
	
	
	/*****************************************************************
	 * Followings are static nested classes used by LocalTupleSpace. *
	 ****************************************************************/
	
	/* 
	 * This class is used for synchronization between threads
	 * if the lock is used, then ref is greater than 0
	 */
	private final static class Lock {
		int ref = 0;
	}
	
	/*
	 * This class is used to store tuples and provides faster pattern matching.
	 * 
	 * Indexes explanation:
	 * indexes[i] is the hash mapping of tuple[i].
	 * indexes[i].get("str") is an array that contains several sets that have 
	 * 		different length of tuple but all of these sets contain the tuple
	 * 		whose ith position is "str".
	 * indexes[i].get("str")[j] is a hash set that contains tuples whose length
	 * 		is j + 1 and the ith string of them is "str".
	 */
	private final static class TupleBase {
		
		final ArrayList<HashSet<Tuple>> base;
		final ArrayList<HashMap<String, ArrayList<HashSet<Tuple>>>> indexes;
		final ArrayList<HashSet<Tuple>> candidates;
		
		TupleBase() {
			base = new ArrayList<HashSet<Tuple>>();
			indexes = new ArrayList<HashMap<String, ArrayList<HashSet<Tuple>>>>();
			candidates = new ArrayList<HashSet<Tuple>>();
		}
		
		public synchronized boolean add(Tuple tpl) {
			// check if base is large enough for the new coming tuple
			while (base.size() < tpl.size()) {
				base.add(new HashSet<Tuple>());
			}
			// add tuple into tuple space
			if (base.get(tpl.size() - 1).add(tpl)) {
				tpl.referredBy(base.get(tpl.size() - 1));
			} else {
				System.err.println("Tuple: " + tpl.toString() + " existed.");
				return false;
			}
			
			// check if indexes is large enough for the new coming tuple
			while (indexes.size() < tpl.size()) {
				indexes.add(new HashMap<String, ArrayList<HashSet<Tuple>>>());
			}
			// update indexes
			String[] t = tpl.getStrings();
			boolean error = false;
			for (int i = 0; i < tpl.size(); i++) {
				ArrayList<HashSet<Tuple>> list = indexes.get(i).get(t[i]);
				if (list == null) {
					list = new ArrayList<HashSet<Tuple>>();
					indexes.get(i).put(t[i], list);
				}
				while (list.size() < tpl.size()) {
					list.add(new HashSet<Tuple>());
				}
				if (list.get(tpl.size() - 1).add(tpl)) {
					tpl.referredBy(list.get(tpl.size() - 1));
				} else {
					error = true;
					System.err.println("Tuple: " + tpl.toString() + 
						"existed in indexes " + i);
				}
			}
			
			// if error happens, clear index of current tuple
			if (error) {
				tpl.clear();
				return false;
			} else 
				return true;
		}
		
		/* 
		 * search tuple that matches pattern in tuple space and remove the 
		 * matched tuple if toRemove is true.
		 * We have to make sure that search and remove operations form an 
		 * atomic operation. Thus, we put tuple-removing here.
		 */
		public synchronized Tuple search(Pattern ptn, boolean toRemove) {
			if (indexes.size() < ptn.size() || 
				base.size() < ptn.size()) return null;
			
			// generate candidate sets and find one who has the minimal size
			candidates.clear();
			
			int iCandidates = 0;
			int sizeMin = Integer.MAX_VALUE;
			int indexMin = 0;
			
			String[] p = ptn.getStrings();
			for (int i = 0; i < p.length; i++) {
				if (p[i] != null) {
					ArrayList<HashSet<Tuple>> list = indexes.get(i).get(p[i]);
					if (list == null || list.size() < p.length ||
						list.get(p.length - 1).isEmpty()) {
						return null;
					} else {
						HashSet<Tuple> hs = list.get(p.length - 1);
						candidates.add(hs);
						if (hs.size() < sizeMin) {
							sizeMin = hs.size();
							indexMin = iCandidates;
						}
						iCandidates++;
					}
				}
			}
			
			// intersection of candidate sets
			if (candidates.size() == 0) {
				HashSet<Tuple> set = base.get(p.length - 1);
				if (set.isEmpty()) {
					return null;
				} else {
					return set.iterator().next();
				}
			} else {
				for (Tuple t : candidates.get(indexMin)) {
					boolean found = true;
					for (int i = 0; i < candidates.size(); i++) {
						if (i != indexMin && !candidates.get(i).contains(t)) {
							found = false;
							break;
						}
					}
					if (found) {
						if (toRemove) t.clear();
						return t;
					}
				}
				return null;
			}
		}
	}
	
	/*
	 * Abstract class that encapsulates String[] and will be inherited by Tuple
	 * and Pattern.
	 */
	private abstract static class StringArray {
		private final String[] strArr;
		
		StringArray(String... strings) {
			strArr = strings.clone();
		}
		
		public String[] getStrings() {
			return strArr;
		}
		
		public int size() {
			return strArr.length;
		}
		
		@Override
		public String toString() {
			return Arrays.toString(strArr);
		}
	}

	private final static class Tuple extends StringArray {
		
		private ArrayList<HashSet<Tuple>> refs;
		
		Tuple(String... tuple) {
			super(tuple);
			// check if tuple contains null
			for (String s : tuple) {
				if (s == null) {
					throw new IllegalArgumentException(" Tuple: " + 
							Arrays.toString(tuple) + " contains null.");
				}
			}
			refs = new ArrayList<HashSet<Tuple>>();
		}
		
		public void referredBy(HashSet<Tuple> set) {
			refs.add(set);
		}
		
		public void clear() {
			for (HashSet<Tuple> set : refs) {
				if (!set.remove(this)) {
					System.err.println("Oops! Wrong references.");
				}
			}
		}
	}

	/*
	 * Unlike tuples where any two tuples should not be equal even they have 
	 * the same length and each string in them is the same, two such patterns 
	 * should be considered as identical. Thus, we need override hashCode and 
	 * equals function here.
	 */
	private final static class Pattern extends StringArray {
		
		public Pattern(String... pattern) {
			super(pattern);
		}

		public boolean matches(Tuple tuple) {
			String[] ptn = getStrings();
			String[] tpl = tuple.getStrings();
			if (ptn.length != tpl.length) return false;
			for (int i = 0; i < ptn.length; i++) {
				if (ptn[i] != null && !ptn[i].equals(tpl[i])) return false;
			}
			return true;
		}
		
		// next two override functions are used by HashMap class 
		@Override
		public int hashCode() {
			int sum = 0;
			String[] ptn = getStrings();
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
			return Arrays.equals(getStrings(), other.getStrings());
		}
	}
}
