//package com.hadoop.marpreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.Object;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class MyAnalysisTask {

	public static class avgMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = transformTextToUTF8(value, "GBK").toString().trim();
			String[] splited = line.split(",");
			String XH = new String();
			String DQSZJ = new String();
			String CJ = new String();

			if (splited[0].compareTo("XH") != 0) {
				if (splited[2] != null) {
					XH = splited[0].trim();
					DQSZJ = splited[1].trim();
					if (splited.length > 3 && splited[3] != null) {
						CJ = splited[3].trim(); // 如果有补考成绩 则替换
					} else {
						CJ = splited[2].trim();
					}
					// TODO:补考成绩<成绩时，是否应该不替换？
					if (CJ.equals("优秀")) {
						CJ = "90";
					} else if (CJ.equals("良好")) {
						CJ = "80";
					} else if (CJ.equals("中等")) {
						CJ = "70";
					} else if (CJ.equals("及格") || CJ.equals("通过")) {
						CJ = "60";
					} else if (CJ.equals("不及格")) {
						CJ = "50";
					} else if (CJ.equals("缓考") || CJ.equals("缺考")
							|| CJ.equals("不通过")) {
						CJ = "0";
						// System.out.println(line);
					}

				} else {
					CJ = "0";
				}

				// 按学号写入 *学号,考试年份 成绩*
				// context.write(new Text(XH + "," + DQSZJ), new Text(CJ));
				context.write(new Text(XH + "-" + DQSZJ), new Text(CJ));
			}
		}

	}

	public static class avgReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Double sum = 0.0;
			int i = 0;
			for (Text text : values) {
				sum += Double.parseDouble(text.toString().trim());
				++i;
			}
			context.write(new Text(key + ","),
					new Text(String.format("%.2f", sum / i)));
		}
	}

	public static class tsgMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text valueText, Context context)
				throws IOException, InterruptedException {
			String line = valueText.toString();
			String[] strs = line.split(",");
			String xm = strs[0];
			String xh = strs[1];
			String year = strs[2];
			String ssh = strs[3];
			String jg = strs[4];

			// context.write(new Text(xh + "," + year + "," + xm), new Text(
			// ssh + "," + jg));
			context.write(new Text(xh + "-" + year + "," + xm), new Text(ssh
					+ "," + jg));
		}
	}

	public static class tsgReducer extends Reducer<Text, Text, Text, Text> {

		private static Map<Character, String> typeMap = new HashMap<Character, String>() {
			private static final long serialVersionUID = 1L;
			{
				put('A', "马克思列宁主义、毛泽东思想、邓小平理论");
				put('B', "哲学、宗教");
				put('C', "社科");
				put('D', "政治、法律");
				put('E', "军事");
				put('F', "经济");
				put('G', "文教科体");
				put('H', "语言文字");
				put('I', "文学");
				put('J', "艺术");
				put('K', "历史地理");
				put('N', "自然科学");
				put('O', "数理科学和化学");
				put('P', "天文学、地球科学");
				put('Q', "生物科学");
				put('R', "医药卫生");
				put('S', "农业科学");
				put('T', "工业技术");
				put('U', "交通运输");
				put('V', "航空航天");
				put('X', "环境科学、安全科学");
				put('Z', "综合性图书");
			}
		};

		// int times = 0;

		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<Character, Integer> numsMap = new TreeMap<Character, Integer>();

			int count = 0;
			int nums = 0;
			// 030470101,林学梅,2013 工业技术,31
			// if (times == 0) {
			// context.write(new Text("学号,年份 ,姓名,看书种类(前四),看书数量,图书馆借书节约了"),
			// new Text(""));
			// ++times;
			// }
			for (Text v : values) {

				String[] ssh_jg = v.toString().split(",");
				if (ssh_jg.length == 0)
					continue;
				// 中图法标识符
				Character c = ssh_jg[0].charAt(0);

				if (numsMap.get(c) == null) {
					numsMap.put(c, 0);
				} else {
					numsMap.put(c, numsMap.get(c) + 1);
				}
				count += Double.parseDouble(ssh_jg[1].trim());
				nums++;
			}
			List<Entry<Character, Integer>> list = new ArrayList<Map.Entry<Character, Integer>>(
					numsMap.entrySet());
			Collections.sort(list,
					new Comparator<Map.Entry<Character, Integer>>() {
						// 降序排序
						@Override
						public int compare(Entry<Character, Integer> o1,
								Entry<Character, Integer> o2) {
							return o2.getValue().compareTo(o1.getValue());
						}

					});

			ArrayList<String> typeList = new ArrayList<>();

			for (Entry<Character, Integer> entry : list) {
				Character c = entry.getKey();
				if (typeMap.get(c) != null)
					typeList.add(typeMap.get(c));
			}

			int to = typeList.size();
			if (to > 4)
				to = 4;
			String types = StringUtils.join("|", typeList.subList(0, to));
			context.write(
					new Text(key + "," + types + "," + nums + "," + count),
					null);

		}
	}

	public static class costMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = transformTextToUTF8(value, "ascii").toString().trim();

			String[] XH_YEAR_COST = line.split(",");

			if (XH_YEAR_COST.length < 3)
				return;
			String XH = XH_YEAR_COST[0];
			String YEAR = XH_YEAR_COST[1];
			String COST = XH_YEAR_COST[2];
			context.write(new Text(XH + "-" + YEAR), new Text(COST));
		}
	}

	public static class costReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Double costsum = 0.0;
			for (Text v : values) {
				costsum += Double.parseDouble(v.toString());
			}
			context.write(new Text(key + "," + String.format("%.2f", costsum)),
					null);
		}
	}

	public static class ioMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = transformTextToUTF8(value, "ascii").toString().trim();
			String[] vs = line.split(",");
			String XH = vs[0];
			String YEAR = vs[1].substring(0, 4);
			String HOUR = vs[2];
			// String MIN = vs[3]; #分钟
			context.write(new Text(XH + "-" + YEAR), new Text(HOUR));
		}
	}

	public static class ioReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int t9 = 0;
			int t11 = 0;
			for (Text v : values) {
				int h = Integer.parseInt(v.toString());
				if (h >= 21 && h <= 23)
					++t9;
				if (h > 23 || h < 6)
					++t11;

			}
			context.write(new Text(key + "," + t9 + "," + t11), null);
		}
	}

	public static class mergeMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] vs = line.split(",", 2);// 分割成XH_YEAR和OTHER
			String XH_YEAR = vs[0];
			String OTHERS = vs[1];
			// 切割传入URL， 获取文件路径名
			String filepath = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			String tag = filepath.split("/")[4];

			context.write(new Text(XH_YEAR), new Text(tag + "-" + OTHERS));
		}
	}

	public static class mergeReducer extends Reducer<Text, Text, Text, Text> {
		int times = 0;

		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String s = new String();
			String ks = key.toString().replace("-", ",");
			// String xh = ks[0];
			// String year = ks[1];

			// 000000284-2006,向师仲,工业技术,2,54 tsg
			// 000000284-2015,4,18 io
			// 000000026-2014,3.40 cost
			// 030360206-2013,46.92 avg
			if (times == 0) {
				context.write(new Text(
						"学号,年份,消费总额,平均分,21点进出,23点进出,姓名,爱看的书,借书数,借书价值总额"),
						new Text(""));
				times++;
			}

			Map<String, String> slist = new HashMap<String, String>() {
				private static final long serialVersionUID = 2L;
				{
					put("cost", "0");
					put("avg", "0");
					put("io", "0,0");
					put("tsg", "0,0,0,0");
				}
			};
			// TODO:还是要每个增加分割
			for (Text v : values) {
				String[] ss = v.toString().split("-", 2);
				String tag = ss[0].trim();
				String others = ss[1];
				if (others != null) {
					slist.put(tag, others);
				}
			}
			// TODO:如何知道是那个文件传进来的？每一条文件加标识符 / context 自带
			List<String> ssList = new ArrayList<String>();
			for (Entry<String, String> entry : slist.entrySet()) {
				ssList.add(entry.getValue());
			}
			String all = StringUtils.join(",", ssList);
			context.write(new Text(ks + "," + all), null);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path baseIn = new Path("hdfs://h1m1:9000/msginput/");
		Path baseOu = new Path("hdfs://h1m1:9000/msgoutput/");
		// TODO: 如何使用CombineClass
		
		Job avgJob = Job.getInstance(conf, avgMapper.class.getSimpleName());
		avgJob.setJarByClass(MyAnalysisTask.class);
		FileInputFormat.setInputPaths(avgJob,
				Path.mergePaths(baseIn, new Path("/XH_YEAR_CJ.csv")));
		avgJob.setMapperClass(avgMapper.class);
		avgJob.setMapOutputKeyClass(Text.class);
		avgJob.setMapOutputValueClass(Text.class);
		avgJob.setReducerClass(avgReducer.class);
		avgJob.setOutputKeyClass(Text.class);
		avgJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(avgJob,
				Path.mergePaths(baseOu, new Path("/avg/")));
		avgJob.waitForCompletion(true);

		Job tsgJob = Job.getInstance(conf, tsgMapper.class.getSimpleName());
		tsgJob.setJarByClass(MyAnalysisTask.class);
		FileInputFormat.setInputPaths(tsgJob,
				Path.mergePaths(baseIn, new Path("/XM_XH_YEAR_SSH_JG.csv")));

		tsgJob.setMapperClass(tsgMapper.class);
		tsgJob.setMapOutputKeyClass(Text.class);
		tsgJob.setMapOutputValueClass(Text.class);
		tsgJob.setReducerClass(tsgReducer.class);
		tsgJob.setOutputKeyClass(Text.class);
		tsgJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(tsgJob,
				Path.mergePaths(baseOu, new Path("/tsg/")));
		tsgJob.waitForCompletion(true);

		Job costJob = Job.getInstance(conf, costMapper.class.getSimpleName());
		costJob.setJarByClass(MyAnalysisTask.class);
		FileInputFormat.setInputPaths(costJob,
				Path.mergePaths(baseIn, new Path("/XH_YEAR_COST.csv")));
		costJob.setMapperClass(costMapper.class);
		costJob.setMapOutputKeyClass(Text.class);
		costJob.setMapOutputValueClass(Text.class);
		costJob.setReducerClass(costReducer.class);
		costJob.setOutputKeyClass(Text.class);
		costJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(costJob,
				Path.mergePaths(baseOu, new Path("/cost/")));
		costJob.waitForCompletion(true);

		Job ioJob = Job.getInstance(conf, ioMapper.class.getSimpleName());
		ioJob.setJarByClass(MyAnalysisTask.class);
		FileInputFormat.setInputPaths(ioJob,
				Path.mergePaths(baseIn, new Path("/XH_DATE_HOUR_MIN.csv")));
		ioJob.setMapperClass(ioMapper.class);
		ioJob.setMapOutputKeyClass(Text.class);
		ioJob.setMapOutputValueClass(Text.class);
		ioJob.setReducerClass(ioReducer.class);
		ioJob.setOutputKeyClass(Text.class);
		ioJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(ioJob,
				Path.mergePaths(baseOu, new Path("/io/")));
		ioJob.waitForCompletion(true);

		Job mergeJob = Job.getInstance(conf, mergeMapper.class.getSimpleName());
		mergeJob.setJarByClass(MyAnalysisTask.class);
		FileInputFormat.setInputPaths(mergeJob,
				Path.mergePaths(baseOu, new Path("/tsg/part-r-00000")),
				Path.mergePaths(baseOu, new Path("/avg/part-r-00000")),
				Path.mergePaths(baseOu, new Path("/cost/part-r-00000")),
				Path.mergePaths(baseOu, new Path("/io/part-r-00000")));
		mergeJob.setMapperClass(mergeMapper.class);
		mergeJob.setMapOutputKeyClass(Text.class);
		mergeJob.setMapOutputValueClass(Text.class);
		mergeJob.setReducerClass(mergeReducer.class);
		mergeJob.setOutputKeyClass(Text.class);
		mergeJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(mergeJob,
				Path.mergePaths(baseOu, new Path("/merge/")));
		mergeJob.waitForCompletion(true);
	}

	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Text(value);
	}
}