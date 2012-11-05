package client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HTablePoolTest {
	
	private static boolean readsDone;
	private static boolean writesDone;
	
	public static void main(String[] args) {
		HTablePool pool = new HTablePool();
		if(args.length != 1) {
			StringBuffer sb = new StringBuffer();
			sb.append("args amount:" + args.length + " must be one argument\n");
			sb.append("\t1: read test\n");
			sb.append("\t2: write test\n");
			sb.append("\t3: read write test\n");
			System.err.println(sb.toString());
			System.exit(1);
		}
		int arg = Integer.parseInt(args[0]);
		String tableName = null;
		switch(arg) {
		
		case 1:
			tableName = "spam_domain_table";
			doRunReadThreads(pool, tableName, true);
			break;
		case 2:
			tableName = "usertable_1";
			doRunWriteThreads(pool);
			readsDone = true; //no reads, set it done directly
			break;
		case 3:
			tableName = "usertable_1";
			doRunReadThreads(pool, tableName, false);
			doRunWriteThreads(pool);
			break;
		default:
			System.err.println("arg:" + arg + " is undefined !!");
			System.exit(1);
		}
		try{} finally {
			while(! readsDone) { 
				/* wait for all threads done*/ 
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			pool.closeTablePool(tableName);
		}
	}

	private static void doRunReadThreads(HTablePool pool, String tableName, boolean check) {
		int totalNum = 500;
		ThreadsCompleteHandler handler = new ThreadsCompleteHandler(
				totalNum, ThreadsCompleteHandler.Type.READ);
		for(int a = 0; a < totalNum; a++) {
			new Thread(new ReadHTableThread(pool, tableName, check, handler), "r" + a).start();
		}
	}

	private static void doRunWriteThreads(HTablePool pool) {
		// run threads
		String tableName = "usertable_1";
		int totalNum = 500;
		ThreadsCompleteHandler handler = new ThreadsCompleteHandler(
				totalNum, ThreadsCompleteHandler.Type.WRITE);
		for(int a = 0; a < totalNum; a++) {
			new Thread(new WriteHTableThread(pool, tableName, "cf1", "col1", handler), "w" + a).start();
		}
		
		while(! writesDone) { 
			/* wait for write threads done*/
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		
		// confirm result
		System.out.println("confirm put data");
		HTableInterface table = pool.getTable(tableName);
		ResultScanner scanner = null;
		Scan scan = new Scan();
		int a = 0;
		try {
			a = 0;
			scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			
			while(it.hasNext()) {
				it.next(); a++;
			}
			
			if(a == totalNum) {
				System.out.println("write result: a:" + a + " == " + totalNum);
			} else {
				throw new IllegalStateException("a:" + a + " != " + totalNum);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(null != scanner) scanner.close();
			pool.putTable(table);
		}
		
		// clean test data
		System.out.println("clean test data");
		table = pool.getTable(tableName);
		List<Delete> deletes = new ArrayList<Delete>();
		Delete delete = null;
		try {
			scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			while(it.hasNext()) {
				delete = new Delete(it.next().getRow());
				deletes.add(delete);
			}
			table.delete(deletes);
			table.flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(null != scanner) scanner.close();
			pool.putTable(table);
		}
	}
	
	static class ReadHTableThread implements Runnable {
		private HTablePool pool;
		private String tableName;
		private boolean check;
		private ThreadsCompleteHandler handler;
		
		public ReadHTableThread(HTablePool pool, String tableName, boolean check, 
				ThreadsCompleteHandler handler) {
			this.pool = pool;
			this.tableName = tableName;
			this.check = check;
			this.handler = handler;
		}
		
		@Override
		public void run() {
			String tName = Thread.currentThread().getName();
			System.out.println("Start tName:" + tName);
			HTableInterface table = this.pool.getTable(this.tableName);
			Scan scan = new Scan();
			ResultScanner scanner = null;
			try {
				scanner = table.getScanner(scan);
				Iterator<Result> it = scanner.iterator();
				int a = 0;
				while(it.hasNext()) {
					it.next();
					a++;
				}
				if(this.check && 500 != a)
					throw new IllegalStateException("a:" + a + " != 500");
				System.out.println("tName:" + tName + " ended with amount:" + a + " records");
				
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				if(null != scanner) scanner.close();
				this.pool.putTable(table);
				this.handler.incrementCompletedThread();
			}
		}
	}
	
	static class ThreadsCompleteHandler {
		private int totalCount;
		private Type type;
		private int completedThreads;
		public static enum Type {READ, WRITE}
		
		public ThreadsCompleteHandler(int totalCount, Type type) {
			this.totalCount = totalCount;
			this.type = type;
		}

		public void incrementCompletedThread() {
			this.completedThreads++;
			if(this.completedThreads >= this.totalCount) {
				if(Type.READ == this.type) {
					readsDone = true;
					System.out.println("readsDone:" + readsDone);
				} else {
					writesDone = true;
					System.out.println("writesDone:" + writesDone);
				}
			}
			System.out.println("amount of completedThreads:" + this.completedThreads + 
					" for type:" + this.type);
		}
	}
	
	static class WriteHTableThread implements Runnable {
		private HTablePool pool;
		private String tableName;
		private String cfName;
		private String cName;
		private ThreadsCompleteHandler handler;
		public WriteHTableThread(HTablePool pool, String tableName, 
				String columnFamilyName, String columnName, ThreadsCompleteHandler handler) {
			this.pool = pool;
			this.tableName = tableName;
			this.cfName = columnFamilyName;
			this.cName = columnName;
			this.handler = handler;
		}
		@Override
		public void run() {
			String tName = Thread.currentThread().getName();
			System.out.println("Start tName:" + tName);
			HTableInterface table = this.pool.getTable(this.tableName);
			Put put = null;
			byte[] rowkey = null;
			try {
				rowkey = Bytes.toBytes(tName);
				put = new Put(rowkey);
				put.add(Bytes.toBytes(this.cfName), Bytes.toBytes(this.cName), rowkey);
				table.put(put);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				System.out.println("tName:" + tName + " ended with put:" + put);
				this.pool.putTable(table);
				this.handler.incrementCompletedThread();
			}
		}
	}
	
	
}
