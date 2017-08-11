import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

    }
}
