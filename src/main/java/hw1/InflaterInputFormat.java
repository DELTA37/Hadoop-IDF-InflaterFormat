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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import java.lang.Math.*;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.Inflater;
import java.util.zip.Deflater;
import java.util.zip.DataFormatException;

import com.google.common.io.LittleEndianDataInputStream;

class InflaterInputFormat extends FileInputFormat<LongWritable, Text> {

	public static class PkzFileSplit extends FileSplit {
		private Long idxOffset;
		private Long idxLength;
		private Long docNumber;
		
		public PkzFileSplit() throws IOException {
			super();
			idxOffset = new Long(-1);
			idxLength = new Long(-1);
			docNumber = new Long(-1);
		}

		public PkzFileSplit(Path file, long start, long length, long idxOffset, long idxLength, long docNumber) {
			super(file, start, length, new String[]{});
			this.idxOffset = new Long(idxOffset);
			this.idxLength = new Long(idxLength);
			this.docNumber = new Long(docNumber);
		} 
		public Long getIdxOffset() {
			return this.idxOffset;
		}
		public Long getIdxLength() {
			return this.idxLength;
		}
		public Long getDocNumber() {
			return this.docNumber;
		}


		 @Override
    public void write(DataOutput out) throws IOException {
    	super.write(out);
     	out.writeLong(this.idxOffset);
     	out.writeLong(this.idxLength);
     	out.writeLong(this.docNumber);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
			this.idxOffset = new Long(in.readLong());
			this.idxLength = new Long(in.readLong());
			this.docNumber = new Long(in.readLong());
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

			long idxOffset = fsplit.getIdxOffset().longValue();
			long idxLength = fsplit.getIdxLength().longValue();
			this.pkzOffset = fsplit.getStart();
			this.docNumber = fsplit.getDocNumber().longValue();
			
			if (idxLength < 0) {
				throw new IOException("idxLength error");
			}

			this.start = 0;
			this.pos = this.start;
			this.end = idxLength / 4l;
			
			long max_length = 0;
			this.lengths = new ArrayList<Long>((int)this.end);

			// read index from start to end
			FileSystem fs = idxPath.getFileSystem(conf);
			FSDataInputStream idxInputTMP = fs.open(idxPath);
			idxInputTMP.seek(idxOffset);
			LittleEndianDataInputStream idxInput = new LittleEndianDataInputStream(idxInputTMP);
			for (int i = 0; i < this.end; i++) {
				long length = idxInput.readInt();
				this.lengths.add(length);
				max_length = Math.max(length, max_length);
			}
			fs = pkzPath.getFileSystem(conf);
      this.finput = fs.open(pkzPath);
			this.finput.seek(this.pkzOffset);	
			this.input_buf = new byte[(int)max_length];
			this.result_buf = new byte[(int)result_buf_max_len];
			
			this.currentKey = new LongWritable(-1);
			this.currentValue = new Text();
		}

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
			if (pos >= end) {
				return false;
			}

			long length = this.lengths.get((int)pos);
			pos += 1;

			this.finput.readFully(this.input_buf, 0, (int)length);

			this.currentKey = new LongWritable(this.pkzOffset);
			this.pkzOffset += length;

			Inflater decompresser = new Inflater();
      decompresser.setInput(this.input_buf, 0, (int)length);
      long result_len = 0;
			long read_len = 0;
			do {
      	try {
					if (result_len == result_buf_max_len) {
						byte[] oldbuf = this.result_buf;
						this.result_buf = new byte[2 * (int)result_buf_max_len];
						System.arraycopy(this.result_buf, 0, oldbuf, 0, (int)result_len);
						result_buf_max_len *= 2;
					}
      		read_len = decompresser.inflate(this.result_buf, (int)result_len, (int)(result_buf_max_len - result_len));
					result_len += read_len;
      	} catch (DataFormatException e) {}
			} while (read_len > 0);
      decompresser.end();
			
			//this.currentValue.set(this.result_buf, 0, (int)result_len);
			this.currentValue = new Text(new String(this.result_buf, 0, (int)result_len, "UTF-8"));
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
			this.finput.close();
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

			long size = fs.getFileStatus(idxPath).getLen() / 4l;
			List<Long> lengths = new ArrayList<>((int)size);
			for (int i = 0; i < size; i++) {
				lengths.add(new Long(idxInput.readInt()));
			}
				
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
					splits.add(new PkzFileSplit(pkzPath, pkzOffset, pkzLength, idxOffset, idxLength, docNumber));

					/*
  				System.out.println(String.format("idxOffset: %d", idxOffset));
	  			System.out.println(String.format("idxLength: %d", idxLength));
		  		System.out.println(String.format("pkzOffset: %d", pkzOffset));
			  	System.out.println(String.format("pkzLength: %d", pkzLength));
					*/

					idxOffset += idxLength;
					pkzOffset += pkzLength;
					idxLength = 0l;
					pkzLength = 0l;
					complete = true;
				}
				docNumber++;
			}

			if (!complete) {
				splits.add(new PkzFileSplit(pkzPath, pkzOffset, pkzLength, idxOffset, idxLength, docNumber));

				/*
				System.out.println(String.format("idxOffset: %d", idxOffset));
				System.out.println(String.format("idxLength: %d", idxLength));
				System.out.println(String.format("pkzOffset: %d", pkzOffset));
				System.out.println(String.format("pkzLength: %d", pkzLength));
				*/

				idxOffset += idxLength;
				pkzOffset += pkzLength;
			  idxLength = 0l;
				pkzLength = 0l;
			}

    }
		return splits;
  }
	
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		InflaterReader reader = new InflaterReader();
		reader.initialize((PkzFileSplit)split, context);
		return reader;
  }
		

	public 	static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";	
	private static long numBytesPerSplit = 16000000l;

	private static long result_buf_max_len = 10000l;

 	public 	static long getNumBytesPerSplit(Configuration conf) {
		return conf.getLong(BYTES_PER_MAP, numBytesPerSplit);
	}
}
