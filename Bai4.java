// Hadoop MapReduce: Phân tích đánh giá theo nhóm tuổi
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Bai4 {
    // Mapper: Join ratings với users để lấy nhóm tuổi, nhả (MovieID_AgeGroup, Rating)
    public static class AgeGroupRatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private HashMap<String, Integer> userIdToAge = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    String path = cacheFile.getPath();
                    if (path.contains("users.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader(path));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length >= 3) {
                                String userId = parts[0].trim();
                                try {
                                    int age = Integer.parseInt(parts[2].trim());
                                    userIdToAge.put(userId, age);
                                } catch (NumberFormatException e) {
                                    // ignore
                                }
                            }
                        }
                        reader.close();
                    }
                }
            }
        }
        private String getAgeGroup(int age) {
            if (age < 18) return "0-18";
            else if (age < 35) return "18-35";
            else if (age < 50) return "35-50";
            else return "50+";
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String userId = fields[0].trim();
                String movieId = fields[1].trim();
                try {
                    float rating = Float.parseFloat(fields[2].trim());
                    Integer age = userIdToAge.get(userId);
                    if (age != null) {
                        String ageGroup = getAgeGroup(age);
                        context.write(new Text(movieId + "_" + ageGroup), new FloatWritable(rating));
                    }
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
    }
    // Reducer: Tính điểm trung bình cho từng phim theo nhóm tuổi

    public static class AgeGroupRatingReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private HashMap<String, String> movieIdToTitle = new HashMap<>();
        // Map<MovieId, Map<AgeGroup, Avg>>
        private Map<String, Map<String, String>> movieAgeGroupToAvg = new TreeMap<>();
        private final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    String path = cacheFile.getPath();
                    if (path.contains("movies.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader(path));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",", 3);
                            if (parts.length >= 2) {
                                String movieId = parts[0].trim();
                                String title = parts[1].trim();
                                movieIdToTitle.put(movieId, title);
                            }
                        }
                        reader.close();
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("_");
            if (parts.length != 2) return;
            String movieId = parts[0];
            String ageGroup = parts[1];
            int count = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            String avgStr = count > 0 ? String.format("%.2f", sum / count) : "NA";
            if (!movieAgeGroupToAvg.containsKey(movieId)) {
                movieAgeGroupToAvg.put(movieId, new TreeMap<>());
            }
            movieAgeGroupToAvg.get(movieId).put(ageGroup, avgStr);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String movieId : movieAgeGroupToAvg.keySet()) {
                String movieTitle = movieIdToTitle.getOrDefault(movieId, movieId);
                Map<String, String> groupMap = movieAgeGroupToAvg.get(movieId);
                StringBuilder sb = new StringBuilder();
                for (String group : AGE_GROUPS) {
                    String avg = groupMap.getOrDefault(group, "NA");
                    sb.append(group).append(": ").append(avg).append("; ");
                }
                // Xóa dấu ; cuối
                if (sb.length() > 2) sb.setLength(sb.length() - 2);
                context.write(new Text(movieTitle), new Text(sb.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: Bai4 <ratings_1.txt> <ratings_2.txt> <movies.txt> <users.txt> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");
        job.setJarByClass(Bai4.class);
        job.setMapperClass(AgeGroupRatingMapper.class);
        job.setReducerClass(AgeGroupRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.addCacheFile(new Path(args[2]).toUri()); // movies.txt
        job.addCacheFile(new Path(args[3]).toUri()); // users.txt
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
