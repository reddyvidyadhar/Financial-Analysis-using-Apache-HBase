package VolHbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main {

	public static void main(String[] args) {

		Configuration conf = HBaseConfiguration.create();
		try {

			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if (admin.isTableAvailable("raw")) {
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			admin.createTable(tableDescriptor);

			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			HTable raw = new HTable(conf, "raw");
			Scan s = new Scan();
			ResultScanner scanner = raw.getScanner(s);
			
			HashMap<String, Double> sym_x = new HashMap<String, Double>();
			HashMap<String, String> symf = new HashMap<String, String>();
			HashMap<String, String> syml = new HashMap<String, String>();
			HashMap<Double, String> s_v = new HashMap<Double, String>();
			HashMap<String, String> sxin = new HashMap<String, String>();
			HashMap<String, Double> smsq = new HashMap<String, Double>();
			List<Double> vol_list = new ArrayList<Double>();
			
			
			Set<String> snam = new HashSet<String>();
			Set<String> sym = new HashSet<String>();

			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				Get get = new Get(result.getRow()); // AAIT10024
				Result entireRow = raw.get(get);

				String snames = new String(entireRow.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
				String years = new String(entireRow.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr")));
				String months = new String(entireRow.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm")));
				String days = new String(entireRow.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd")));
				String prices = new String(entireRow.getValue(Bytes.toBytes("price"), Bytes.toBytes("price")));

				int day = Integer.parseInt(days);
				String st_yr_mn = snames + "," + years + "," + months;

				if (syml.containsKey(st_yr_mn)) {
					int past_date = Integer.parseInt(syml.get(st_yr_mn).split(",")[0]);
					if (day < past_date) {
						syml.put(st_yr_mn, String.valueOf(day) + "," + prices);
					}
				} else {
					syml.put(st_yr_mn, days + "," + prices);
				}

				if (symf.containsKey(st_yr_mn)) {
					int past_date = Integer.parseInt(symf.get(st_yr_mn).split(",")[0]);
					if (day > past_date) {
						symf.put(st_yr_mn, String.valueOf(day) + "," + prices);
					}
				} else {
					symf.put(st_yr_mn, days + "," + prices);
				}
				// symd_p.put(snames + "," + years+ ","+ months+ "," + days, prices); // <Key:AAIT,2014,02,25;Value:31.84>
				sym.add(snames + "," + years + "," + months); // AAIT,2014,02
				snam.add(snames);

			}
			
			for (String k : sym) {
				double x_i = 0.0;
				String stock = k.split(",")[0];
				double last = Double.parseDouble(syml.get(k).split(",")[1]);
				double first = Double.parseDouble(symf.get(k).split(",")[1]);
				double sum_xi = 0.0;
				int index = 0;

				x_i = (first - last) / last;
				sym_x.put(k, x_i);
				if (sxin.containsKey(stock)) {
					index = Integer.parseInt(sxin.get(stock).split(",")[1]) + 1;

					sum_xi = Double.parseDouble(sxin.get(stock).split(",")[0]);
					sum_xi = sum_xi + x_i;

					sxin.put(stock,String.valueOf(sum_xi) + ","+ String.valueOf(index)); // Key: <"AAIT", "2.456,36">
				} else {
					sxin.put(stock, String.valueOf(x_i) + ",1");
				}
			}
								
			for (String k : sym_x.keySet()) {
				String stock = k.split(",")[0];
				double sum_xi = Double.parseDouble(sxin.get(stock).split(",")[0]);
				int noOfmonths = Integer.parseInt(sxin.get(stock).split(",")[1]);
				
				double x_bar = sum_xi/noOfmonths;
				
				double xixbarsq = (sym_x.get(k) - x_bar) * (sym_x.get(k) - x_bar);
				
				if (smsq.containsKey(stock)) {
					double sumOfxi_xbarsq = smsq.get(stock) + xixbarsq;
					smsq.put(stock, sumOfxi_xbarsq);
				} else {
					smsq.put(stock, xixbarsq);
				}
			}
			
			for (String stock : smsq.keySet()) {
				
				int index = Integer.parseInt(sxin.get(stock).split(",")[1]);
				
				if (index > 1) {
					double temp = smsq.get(stock) / (index - 1);
					double vol = Math.sqrt(temp);
					if (vol > 0.0) {
						s_v.put(vol,stock);
						vol_list.add(vol);
					}
				}

			}
			
			System.out.println("########## Top 10 Min Volatility ###############");
			
			Collections.sort(vol_list);
			for(int i=0;i<10;i++){
				Double value = vol_list.get(i);
				String name =s_v.get(value);
				
				System.out.println(name+"\t"+Double.toString(value));
			}

			System.out.println("########## Top 10 Max Volatility ###############");
			Collections.reverse(vol_list);
			
			for(int i=0;i<10;i++){
				Double value = vol_list.get(i);
				String name =s_v.get(value);
				
				System.out.println(name+"\t"+Double.toString(value));
			}
			raw.close();
			admin.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}