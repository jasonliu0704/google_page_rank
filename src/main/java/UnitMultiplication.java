import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {


    // construct and process transition matrix
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String line = value.toString().trim();
            String[] fromAndTo = line.split("\t");

            // dead end no outgoing edges
            if(fromAndTo.length == 1 || fromAndTo[1].trim().equals("")){
                return;
            }

            String from = fromAndTo[0];
            String[] tos = fromAndTo[1].split(",");
            for(String to: tos){
                // output key=from, value= to=prob
                context.write(new Text(from), new Text(to + "=" + (double)1/tos.length));
            }

        }
    }

    // process and construct page rank matrix
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] pageRank = value.toString().trim().split("\t");
            context.write(new Text(pageRank[0]), new Text(pageRank[1]));
        }
    }

    // process and calculate subPr for to page
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            double pr = 0;
            List<String> transitionUnit = new ArrayList<String>();
            for(Text val: values){
                if(val.toString().contains("=")){
                    transitionUnit.add(value.toString());
                }else{
                    pr = Double.parseDouble(value.toString());
                }
            }

            // multiply to get subpr
            for(String toPage: transitionUnit){
                String outputkey = toPage.split("=")[0];
                double weight = Double.parseDouble(toPage.split("=")[1]);
                String outputValue = String.valueOf(weight * pr);
                context.write(new Text(outputkey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //chain two mapper classes
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
