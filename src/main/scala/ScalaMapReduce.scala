import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.lang;
import collection.JavaConverters._



object ScalaMapReduce extends Configured with Tool {
  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }
//  маппер который меняет местами пример id 2 => 2 id
//  hadoop jar ScalaMapReduce-assembly-0.1.jar \
//    -Dswap.input=/user/hduser/ppkm_out \
//    -Dswap.output=/user/hduser/ppkm_swap

//  mapper мы передаем ключ значение что вводим, это первая пара, и что хотим получать на выходе пара ключ значение
  class VovkinaMapa extends Mapper[Object, Text,  Text, IntWritable] {
    val text = new Text()
    val amount = new IntWritable(1)
    override def map(key: Object, value: Text,
                     context: Mapper[Object, Text,  Text, IntWritable]#Context): Unit = {
      val kv = value.toString.split("\\s")
//      код из мап java
//      for (String word: words)
//      {
//        text.set(word);
//        context.write(text, one);
//      }
      for (word <- kv){
        text.set(word)
        context.write(text, amount)
      }
//      word.set(kv(0))
//      amount.set(kv(1).toInt)
//      context.write(amount, word)
    }
  }

  class VovkinReducer extends Reducer[Text, IntWritable, Text, IntWritable]{
    val summa_blin = new IntWritable()
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      for (i <- values.asScala) {
        sum +=i.get()
      }
      summa_blin.set(sum)
      context.write(key, summa_blin)
    }
  }

  val IN_PATH_PARAM = "swap.input"
  val OUT_PATH_PARAM = "swap.output"
  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word Count in scala")
    job.setJarByClass(getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[VovkinaMapa])
    job.setReducerClass(classOf[VovkinReducer])
    job.setNumReduceTasks(1)
    val in = new Path(getConf.get(IN_PATH_PARAM))
    val out = new Path(getConf.get(OUT_PATH_PARAM))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    val fs = FileSystem.get(getConf)
    if (fs.exists(out)) fs.delete(out, true)
    if (job.waitForCompletion(true)) 0 else 1
  }
}
