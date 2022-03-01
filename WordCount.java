//libraries
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

//Main class
public class WordCount {

	//Reduce txt file codes
    public static class reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    	//Reducer� olu�turma
        private IntWritable WReduce = new IntWritable();
        //reduce function
        public reduce(Text myKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	//reducer i�in kelimeleri sayd�rma d�ng�s�
            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }

            WReduce.set(total);
            context.write(myKey, WReduce);
        }
    }
    //Map txt file codes
    public static class map extends Mapper<Object, Text, Text, IntWritable> {
    	//New Mapper
        private final static IntWritable myMapper = new IntWritable(1);
        //Text file
        private Text WMap = new Text();
        //Mapper function
        public map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//ayn� kelimeleri bulmak i�in mapper kulan�l�r. bunu yaparken tokenizer fonksiyonu bize yard�mc� olur
            StringTokenizer tkn = new StringTokenizer(value.toString());
            //ayn� kelimeden birden fazla varsa i�aretle ve b�rak , ayn� kelimeyi birden fazla kez yazd�rma d�ng�s�
            while (tkn.hasMoreTokens()) {
                WMap.set(tkn.nextToken());
                context.write(WMap, myMapper);
            }
        }
    }
    //output codes
    public static void main(String[] args) throws Exception {
    	//WordCount s�n�f� i�in bir config yap�land�rmas� olu�turma
        Configuration cfg = new Configuration();
        //olu�turulan config dosyas�n� Job i�erisinde kullanarak job fonksiyonunun niteli�ini belirtmek
        Job job = new Job(cfg, "WordCount");
        //Olu�turulan jar� tan�mlama
        job.setJarByClass(WordCount.class);
        //mapper i�in map clas�n� tan�mlama
        job.setMapperClass(map.class);
        //reducer i�in reduce clas�n� tan�mlama
        job.setCombinerClass(reduce.class);
        job.setReducerClass(reduce.class);
        // yazma i�lemleri i�in ��k�� dosyalar�n� tan�mlama (��k�� dosyas�n�n b�y�kl���ne g�re ��k�� verileri birden fazla par�aya b�l�nebilir )
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //Okuma i�lemleri i�in giri� dosyalar�n� tan�mlama
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //yazd�rma i�lemi tamamland���nda fonksiyonu sona erdirme
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}