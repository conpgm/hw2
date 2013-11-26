package tuplespaces;

import java.util.ArrayList;

public class LocalTupleSpace implements TupleSpace {
	
	ArrayList<String[]> space;
	ArrayList<String[]> waiting;
//	int count =0;
	public LocalTupleSpace () {
		space = new ArrayList<String[]>();
		waiting = new ArrayList<String[]>();
	}
	
//	public synchronized void printPattern(String[] pattern, String name, int myCount){
//		System.out.print("count="+myCount+"  "+name+": ");
//		for(String item : pattern){
//			System.out.print(item);
//		}
//		System.out.println();
//	}
	
//	public synchronized void printSpace(String name, int myCount){
//		System.out.println("***************************");
//		for(String[] pattern : this.space){
//			this.printPattern(pattern, name, myCount);
//		}
//		System.out.println("***************************");
//	}
	
	public String[] get(String... pattern) {
		String[] tp;
		String[] wp = addWaiting(pattern);
//		int myCount;
//		synchronized(this){
//			count++;
//			myCount=count;
//		}
		synchronized(wp){
			while ((tp = search(pattern)) == null) {
			//synchronized (wp){
				try {
					
					//System.out.println(count+"  22");
//					this.printPattern(pattern, "get", myCount);
					wp.wait();
//					System.out.println(myCount +"  23");
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
			}
		}
		//System.out.println("count="+count+" 31");
		synchronized (this) {
//			this.printPattern(pattern, "remove", myCount);
			//this.printSpace("remove", myCount);
			space.remove(tp);
//			this.printSpace("remove", myCount);
		}
		//System.out.println("count="+count+" 36");
		return tp;
	}

	public String[] read(String... pattern) {
		String[] tp;
		String[] wp = addWaiting(pattern);
//		int myCount;
//		synchronized(this){
//			count++;
//			myCount = count;
//		}
		synchronized(wp){
			while ((tp = search(pattern)) == null) {
			//synchronized (wp) {
				try {
					//System.out.println(count+"   64");
//					this.printPattern(pattern, "read", myCount);
					wp.wait();
					//System.out.println(count);
					//this.printPattern(pattern,);
//					System.out.println(66);
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
			//}
			}
		}
		return tp;
	}

	public void put(String... tuple) {
//		int myCount;
		synchronized (this) {
//			count++;
//			myCount = count;
			//System.out.println(count+"  82");
			space.add(tuple.clone());
//			this.printPattern(tuple,"put", myCount);
		}
		
		String[] wp = notifyWaiting(tuple);
		if(wp != null){
			synchronized(wp){
				wp.notify();
			}
		}
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
				if (w[i] != null && w[i] != tuple[i]) {
					found = false;
					break;
				}
			}
			if (found) return w;
		}
		return null;
	}
}
