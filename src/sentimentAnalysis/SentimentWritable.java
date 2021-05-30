package sentimentAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SentimentWritable implements Writable {
    private final IntWritable ratio;
    private final IntWritable length;

    public SentimentWritable() {
        this.ratio = new IntWritable(0);
        this.length = new IntWritable(0);
    }

    public SentimentWritable(IntWritable ratio, IntWritable len) {
        this.ratio = ratio;
        this.length = len;
    }

    public IntWritable getRatio() {
        return this.ratio;
    }

    public IntWritable getLength() {
        return this.length;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.length.write(dataOutput);
        this.ratio.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.length.readFields(dataInput);
        this.ratio.readFields(dataInput);
    }
}