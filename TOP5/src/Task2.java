import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Task2 extends Configured implements Tool {

    static int printUsage() {
        System.out.println("task2 [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class Job1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text driver = new Text();
        private final static DoubleWritable revenue = new DoubleWritable(0);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] args = line.split(",");

            driver.set(args[1]);

            try {
                Double parsed_revenue = Double.parseDouble(args[10]);
                revenue.set(parsed_revenue);
            } catch (NumberFormatException e) {
                return;
            }
            context.write(driver, revenue);
        }
    }

    public static class Job1Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Job2Mapper extends Mapper<Object, Text, Text, Text> {
        private PriorityQueue<DriverRevenue> queue;
        public void setup(Context context) {
            queue = new PriorityQueue<>(5, new Comparator<DriverRevenue>() {
                public int compare(DriverRevenue d1, DriverRevenue d2) {
                    return Double.compare(d1.revenue, d2.revenue);
                }
            });
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] args = value.toString().split("\\s+");
            String driver = args[0];
            double revenue = Double.parseDouble(args[1]);

            queue.offer(new DriverRevenue(driver, revenue));
            if (queue.size() > 5) {
                queue.poll();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                DriverRevenue driver = queue.poll();
                context.write(new Text("1"), new Text(driver.toString()));
            }
        }
    }

    public static class Job2Reducer extends Reducer<Text,Text,Text,Text> {
        private PriorityQueue<DriverRevenue> queue;
        public void setup(Context context) {
            queue = new PriorityQueue<>(5, new Comparator<DriverRevenue>() {
                public int compare(DriverRevenue d1, DriverRevenue d2) {
                    return Double.compare(d1.revenue, d2.revenue);
                }
            });
        }

        private Text driver = new Text();
        private Text revenue = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] args = value.toString().split("=");
                String driver = args[0];
                double revenue = Double.parseDouble(args[1]);
                queue.offer(new DriverRevenue(driver, revenue));
                if (queue.size() > 5) {
                    queue.poll();
                }
            }

            while (queue.size() > 0) {
                DriverRevenue driverRevenue = queue.poll();
                driver.set(driverRevenue.driver);
                revenue.set(String.valueOf(driverRevenue.revenue));
                context.write(driver, revenue);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job1");
        job1.setJarByClass(Task2.class);
        job1.setMapperClass(Job1Mapper.class);
        job1.setReducerClass(Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job1.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job1, other_args.get(0));
        Path job1_output = new Path(other_args.get(1) + "job1");
        FileOutputFormat.setOutputPath(job1, job1_output);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job2");
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, job1_output);
        FileOutputFormat.setOutputPath(job2, new Path(other_args.get(1)));
        return (job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task2(), args);
        System.exit(res);
    }

}

class DriverRevenue {
    public String driver;
    public double revenue;

    public DriverRevenue(String driver, double revenue) {
        this.driver = driver;
        this.revenue = revenue;
    }

    public String toString() {
        return this.driver + "=" + this.revenue;
    }
}
