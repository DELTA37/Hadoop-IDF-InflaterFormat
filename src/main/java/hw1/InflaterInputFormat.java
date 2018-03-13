package hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;

import java.lang.Math.*;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.Inflater;
import java.util.zip.Deflater;
import java.util.zip.DataFormatException;

import com.google.common.io.LittleEndianDataInputStream;

class InflaterInputFormat extends FileInputFormat<LongWritable, Text> {

	public static class PkzFileSplit extends FileSplit {
		private long idxOffset;
		private long idxLength;
		private long docNumber;
		
		public PkzFileSplit() {
			super();
			this.idxOffset = 0l;
			this.idxLength = 0l;
			this.docNumber = 0l;
		}

		public PkzFileSplit(Path file, long start, long length, String[] hosts) {
			super(file, start, length, hosts);
		}
		
		public PkzFileSplit(Path file, long start, long length, String[] hosts, long idxOffset, long idxLength, long docNumber) {
			super(file, start, length, hosts);
			this.idxOffset = idxOffset;
			this.idxLength = idxLength;
			this.docNumber = docNumber;
		} 
		public long getIdxOffset() {
			return this.idxOffset;
		}
		public long getIdxLength() {
			return this.idxLength;
		}
		public long getDocNumber() {
			return this.docNumber;
		}
	}

  public class InflaterReader extends RecordReader<LongWritable, Text> {
		long start;
		long pos;
		long end;
		
		long docNumber;
		long pkzOffset;

		byte[] input_buf;
		byte[] result_buf;
		
		List<Long> lengths;

		FSDataInputStream finput;

		LongWritable currentKey;
		Text currentValue;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    	Configuration conf = context.getConfiguration();
      PkzFileSplit fsplit = (PkzFileSplit)split;

      Path pkzPath = fsplit.getPath();
      Path idxPath = pkzPath.suffix(".idx");

			long idxOffset = fsplit.getIdxOffset();
			long idxLength = fsplit.getIdxLength();
			this.pkzOffset = fsplit.getStart();

			this.docNumber = fsplit.getDocNumber();

			this.start = 0;
			this.pos = 0;
			this.end = idxLength;
			
			long max_length = 0;	
			this.lengths = new ArrayList<>();

			// read index from start to end
			FileSystem fs = idxPath.getFileSystem(conf);
			FSDataInputStream idxInputTMP = fs.open(idxPath);
			idxInputTMP.seek(idxOffset);
			LittleEndianDataInputStream idxInput = new LittleEndianDataInputStream(idxInputTMP);
			for (int i = 0; i < idxLength; i++) {
				long length = idxInput.readInt();
				this.lengths.add(length);
				max_length = Math.max(length, max_length);
			}

			fs = pkzPath.getFileSystem(conf);
      this.finput = fs.open(pkzPath);
			this.finput.seek(this.pkzOffset);	

			this.input_buf = new byte[(int)max_length];
			this.result_buf = new byte[(int)result_buf_len];
		}

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
			if (pos >= end) {
				return false;
			}

			long length = this.lengths.get((int)pos);
			pos++;

			this.finput.readFully(this.input_buf, 0, (int)length);

			this.currentKey = new LongWritable(this.pkzOffset + length);

			Inflater decompresser = new Inflater();
      decompresser.setInput(this.input_buf, 0, (int)length);
      long result_len = 0;
      try {
      	result_len = decompresser.inflate(this.result_buf);
				if (result_len > result_buf_len) {
					throw new DataFormatException();
				}
      } catch (DataFormatException e) {
        e.printStackTrace();
      }
      decompresser.end();

			this.currentValue = new Text(new String(this.result_buf, 0, (int)result_len, "UTF-8"));
			this.currentValue = new Text("Hi everyone");
			//this.currentValue = new Text(new String(this.result_buf, 0, (int)result_len, "UTF-16"));

			return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
    	return this.currentKey;  
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
    	return this.currentValue;  
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
			return ((float)this.pos) / this.end;
    }

    @Override
    public void close() throws IOException {
			IOUtils.closeStream(this.finput);
			//this.finput.close();
    }

  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = new ArrayList<>();

		long docNumber = 0;
		long bytesPerSplit = getNumBytesPerSplit(context.getConfiguration());

    for (FileStatus status : this.listStatus(context)) {
      Path pkzPath = status.getPath();
			if (!pkzPath.toString().endsWith(".pkz")) {
				continue;
			}

      Path idxPath = pkzPath.suffix(".idx");
      Configuration conf = context.getConfiguration();
			FileSystem fs = idxPath.getFileSystem(conf);
			LittleEndianDataInputStream idxInput = new LittleEndianDataInputStream(fs.open(idxPath));
			List<Long> lengths = new ArrayList<>();
			try {
				while(true) {
					lengths.add(new Long(idxInput.readInt()));
				}
			} catch (IOException e) {}
				
			long pkzOffset = 0l;
			long pkzLength = 0l;
			long idxOffset = 0l;
			long idxLength = 0l;

			boolean complete = false;
			for (int i = 0; i < lengths.size(); i++) {
				pkzLength += lengths.get(i);
				idxLength += 4l;
				complete = false;
				if (pkzLength > bytesPerSplit) {
					splits.add(new PkzFileSplit(pkzPath, pkzOffset, pkzLength, null, idxOffset, idxLength, docNumber));

  				System.out.println(String.format("idxOffset: %d", idxOffset));
	  			System.out.println(String.format("idxLength: %d", idxLength));
		  		System.out.println(String.format("pkzOffset: %d", pkzOffset));
			  	System.out.println(String.format("pkzLength: %d", pkzLength));

					idxOffset += idxLength;
					pkzOffset += pkzLength;
					pkzLength = 0l;
					idxLength = 0l;
					complete = true;
				}
				docNumber++;
			}
			if (!complete) {
				splits.add(new PkzFileSplit(pkzPath, pkzOffset, pkzLength, null, idxOffset, idxLength, docNumber));
				System.out.println(String.format("idxOffset: %d", idxOffset));
				System.out.println(String.format("idxLength: %d", idxLength));
				System.out.println(String.format("pkzOffset: %d", pkzOffset));
				System.out.println(String.format("pkzLength: %d", pkzLength));

				idxOffset += idxLength;
				pkzOffset += pkzLength;
				pkzLength = 0l;
			  idxLength = 0l;
			}
    }
		return splits;
  }
	
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		InflaterReader reader = new InflaterReader();
		reader.initialize(split, context);
		return reader;
  }
		

	public static final String BYTES_PER_MAP = "mapreduce.input.bmp.bytes_per_map";	
	private static long numBytesPerSplit = 1600000;
	private static LongWritable one = new LongWritable(1);

	private static long result_buf_len = 10000l;

 	public static long getNumBytesPerSplit(Configuration conf) {
		return conf.getLong(BYTES_PER_MAP, numBytesPerSplit);
	}
}
