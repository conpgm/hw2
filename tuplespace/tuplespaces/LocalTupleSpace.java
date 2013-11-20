package tuplespaces;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

class Tuple {
	private String[] data;
	private ArrayList<HashSet<Tuple>> refs;
	private int length;
	
	public Tuple (String... data) {
		this.data = data.clone();
		length = data.length;
		refs = new ArrayList<HashSet<Tuple>>();
	}
	
	public String[] getData() {
		return data;
	}
	
	public int size() {
		return length;
	}
	
	public void ref(HashSet<Tuple> set) {
		refs.add(set);
	}
	
	public void unref() {
		if (refs.size() != length + 1) {
			System.err.println("Oops! Wrong count number of references.");
		}
		
		for (HashSet<Tuple> set : refs) {
			if (!set.remove(this)) {
				System.err.println("Oops! Exception occured.");
				return ;
			}
		}
	}
}

public class LocalTupleSpace implements TupleSpace {
	
	ArrayList<HashSet<Tuple>> space;
	ArrayList<HashMap<String, ArrayList<HashSet<Tuple>>>> indexes;
	ArrayList<HashSet<Tuple>> candidateSets;
	
	public LocalTupleSpace () {
		space = new ArrayList<HashSet<Tuple>>();
		indexes = new ArrayList<HashMap<String, ArrayList<HashSet<Tuple>>>>();
		candidateSets = new ArrayList<HashSet<Tuple>>();
	}
	
	public synchronized String[] get(String... pattern) {
		Tuple tp;
		while ((tp = search(pattern)) == null) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
		
		tp.unref();
		return tp.getData();
	}

	public synchronized String[] read(String... pattern) {
		Tuple tp;
		while ((tp = search(pattern)) == null) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
		
		return tp.getData();
	}

	public synchronized void put(String... tuple) {	
		Tuple tp = new Tuple(tuple);

		// check if tuple space is long enough for the new coming tuple
		while (space.size() < tp.size()) {
			HashSet<Tuple> set = new HashSet<Tuple>();
			space.add(set);
		}
		// add tuple into tuple space
		if (space.get(tp.size() - 1).add(tp)) {
			tp.ref(space.get(tp.size()-1));
		} else {
			System.err.println(Arrays.toString(tuple) + "existed in tuple space.");
			return ;
		}
		
		// check if indexes is long enough for the new coming tuple
		while (indexes.size() < tp.size()) {
			HashMap<String, ArrayList<HashSet<Tuple>>> map = 
					new HashMap<String, ArrayList<HashSet<Tuple>>>();
			indexes.add(map);
		}
		
		// update indexes
		for (int i = 0; i < tp.size(); i++) {
			ArrayList<HashSet<Tuple>> list = indexes.get(i).get(tuple[i]);
			if (list == null) {
				list = new ArrayList<HashSet<Tuple>>();
				indexes.get(i).put(tuple[i], list);
			}
			while (list.size() < tp.size()) {
				HashSet<Tuple> set = new HashSet<Tuple>();
				list.add(set);
			}
			if (list.get(tp.size() - 1).add(tp)) {
				tp.ref(list.get(tp.size() - 1));
			} else {
				System.err.println(Arrays.toString(tuple) + "existed in indexes " + i);
			}
		}
		
		notifyAll();
	}
	
	private Tuple search(String... pattern) {
		
		if (indexes.size() < pattern.length || space.size() < pattern.length) return null;
		
		// generate candidate tuple sets and find the set who has the minimum size
		candidateSets.clear();
		
		int iCandidateSets = 0;
		int sizeMin = Integer.MAX_VALUE;
		int indexMin = 0;
		
		for (int i = 0; i < pattern.length; i++) {
			if (pattern[i] != null) {
				ArrayList<HashSet<Tuple>> list = indexes.get(i).get(pattern[i]);
				if (list == null || list.size() < pattern.length || list.get(pattern.length - 1).isEmpty()) {
					return null;
				} else {
					HashSet<Tuple> set = list.get(pattern.length - 1);
					candidateSets.add(set);
					if (set.size() < sizeMin) {
						sizeMin = set.size();
						indexMin = iCandidateSets;
					}
					iCandidateSets++;
				}
			}
		}
		
		// intersection of candidate tuple sets
		if (candidateSets.size() == 0) {
			HashSet<Tuple> set = space.get(pattern.length - 1);
			if (set.isEmpty()) {
				return null;
			} else {
				return set.iterator().next();
			}
		} else {
			for (Tuple tp : candidateSets.get(indexMin)) {
				boolean found = true;
				for (int i = 0; i < candidateSets.size(); i++) {
					if (i != indexMin && !candidateSets.get(i).contains(tp)) {
						found = false;
						break;
					}
				}
				if (found) return tp;
			}
			return null;
		}
	}
}
