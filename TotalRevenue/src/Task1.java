
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Task1 extends Configured implements Tool {

    static int printUsage() {
        System.out.println("task1 [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class Task1Mapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text date = new Text();
        private final static DoubleWritable revenue = new DoubleWritable(0);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] args = line.split(",");

            date.set(args[3].substring(0, 11));

            try {
                Double parsed_revenue = Double.parseDouble(args[10]);
                revenue.set(parsed_revenue);
            } catch (NumberFormatException e) {
                return;
            }
            context.write(date, revenue);
        }
    }

    public static class Task1Reducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
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

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(Task1Mapper.class);
        job.setCombinerClass(Task1Reducer.class);
        job.setReducerClass(Task1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
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
        FileInputFormat.setInputPaths(job, other_args.get(0));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task1(), args);
        System.exit(res);
    }

}

