import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        // mapper for transition matrix

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read relation file
            // inputs: 1\t3,4,5
            // output key: from:1
            // output value: to=prob: 3=1/3

            String line = value.toString().trim();
            String[] relationArry = line.split("\t");

            if (relationArry.length < 2) {
                // logger.debug("found dead ends: " + relationArry[0]);
                return;
            }

            String outputKey = relationArry[0];
            String[] targets = relationArry[1].split(",");
            for (String target : targets) {
                context.write(new Text(outputKey), new Text(target + "=" + (double)1/targets.length));
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        // mapper for PR matrix

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // inputs: 1\t1
            String[] pr = value.toString().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input key = fromId : 1
            // input value = <toId=prob, toId=prob, ... , pr>
            // output key = toId
            // output value = prob * pr
            double pr = 0;
            List<String> transCells = new ArrayList<String>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transCells.add(value.toString());
                }
                else {
                    pr = Double.parseDouble(value.toString());
                }
            }

            for (String transCell : transCells) {
                String toId = transCell.split("=")[0];
                double prob = Double.parseDouble(transCell.split("=")[1]);
                double subPr = prob * pr;

                context.write(new Text(toId), new Text(String.valueOf(subPr)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(UnitMultiplication.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
 }
