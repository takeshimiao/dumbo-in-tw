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
			pool.closeTablePool(tableName);
			break;
		case 2:
			doRunWriteThreads(pool);
			pool.closeTablePool("usertable_1");
			break;
		case 3:
			tableName = "usertable_1";
			doRunReadThreads(pool, tableName, false);
			doRunWriteThreads(pool);
			pool.closeTablePool(tableName);
			break;
		default:
			System.err.println("arg:" + arg + " is undefined !!");
			System.exit(1);
		}
	}

	private static void doRunReadThreads(HTablePool pool, String tableName, boolean check) {
		for(int a = 0; a < 500; a++) {
			new Thread(new ReadHTableThread(pool, tableName, check), "r" + a).start();
		}
	}

	private static void doRunWriteThreads(HTablePool pool) {
		// run threads
		String tableName = "usertable_1";
		for(int a = 0; a < 500; a++) {
			new Thread(new WriteHTableThread(pool, tableName, "cf1", "col1"), "w" + a).start();
		}
		
		// confirm result
		System.out.println("confirm put data");
		HTableInterface table = pool.getTable(tableName);
		Scan scan = new Scan();
		int a = 0;
		try {
			Thread.sleep(10000);
			
			a = 0;
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			
			while(it.hasNext()) {
				it.next(); a++;
			}
			scanner.close();
			pool.putTable(table);
			
			if(a == 500) {
				System.out.println("write result: a:" + a + " == 500");
			} else {
				throw new IllegalStateException("a:" + a + " != 500");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		// clean test data
		System.out.println("clean test data");
		table = pool.getTable(tableName);
		ResultScanner scanner;
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
		}
		pool.putTable(table);
	}
	
	static class ReadHTableThread implements Runnable {
		private HTablePool pool;
		private String tableName;
		private boolean check;
		public ReadHTableThread(HTablePool pool, String tableName) {
			this(pool, tableName, true);
		}
		
		public ReadHTableThread(HTablePool pool, String tableName, boolean check) {
			this.pool = pool;
			this.tableName = tableName;
			this.check = check;
		}
		@Override
		public void run() {
			String tName = Thread.currentThread().getName();
			System.out.println("Start tName:" + tName);
			HTableInterface table = this.pool.getTable(this.tableName);
			Scan scan = new Scan();
			try {
				ResultScanner scanner = table.getScanner(scan);
				Iterator<Result> it = scanner.iterator();
				Result rs = null;
				int a = 0;
				while(it.hasNext()) {
					rs = it.next();
					a++;
				}
				if(this.check && 500 != a)
					throw new IllegalStateException("a:" + a + " != 500");
				System.out.println("tName:" + tName + " ended with amount:" + a + " records");
				
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			this.pool.putTable(table);
		}
	}
	
	static class WriteHTableThread implements Runnable {
		private HTablePool pool;
		private String tableName;
		private String cfName;
		private String cName;
		public WriteHTableThread(HTablePool pool, String tableName, String columnFamilyName, String columnName) {
			this.pool = pool;
			this.tableName = tableName;
			this.cfName = columnFamilyName;
			this.cName = columnName;
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
			}
			System.out.println("tName:" + tName + " ended with put:" + put);
			this.pool.putTable(table);
		}
	}
	
	
}
