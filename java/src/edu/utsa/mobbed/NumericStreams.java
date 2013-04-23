package edu.utsa.mobbed;

import java.io.*;
import java.sql.*;
import java.util.UUID;
import java.nio.ByteBuffer;

import org.postgresql.copy.CopyManager;

/**
 * Handler class for NUMERIC_STREAM table. This class contains functions to
 * store, retrieve or delete records from NUMERIC_DATA table. The insertion and
 * retrieval functions are designed to work in a multi-threaded approach. For
 * example, two separate thread are used for writing the data. One thread
 * connects to the database and opens an InputStream connected to the table
 * while another thread opens an OutputStream to write the data in binary to the
 * InputStream provided by the previous thread. The retrieval process functions
 * in the same manner.
 * 
 * @author Arif Hossain, Kay Robbins
 * 
 */
public class NumericStreams {

	/**
	 * This class reads data in binary from database and writes it to a
	 * PipedOutputStream. It implements runnable interface and run on a separate
	 * execution thread.
	 * 
	 * @author Arif Hossain, Kay Robbins
	 * 
	 */
	class ReadBinaryData implements Runnable {
		UUID datadefUuid;
		int endPosition;
		PipedOutputStream pout;
		int startPosition;

		/**
		 * Creates a ReadBinaryData object.
		 * 
		 * @param pout
		 *            PipedOutputStream to write the data.
		 * @param datadefUuid
		 *            UUID of the DataDef
		 * @param startPosition
		 *            Start time for retrieval
		 * @param endPosition
		 *            End time for retrieval
		 */
		public ReadBinaryData(PipedOutputStream pout, UUID datadefUuid,
				int startPosition, int endPosition) {
			this.pout = pout;
			this.datadefUuid = datadefUuid;
			this.startPosition = startPosition;
			this.endPosition = endPosition;
		}

		/**
		 * Execution thread to read data from database.
		 */
		public void run() {
			try {
				CopyManager copy = ((org.postgresql.PGConnection) dbCon)
						.getCopyAPI();
				DataOutputStream dos = new DataOutputStream(pout);
				String copyQry = "COPY (select NUMERIC_STREAM_DATA_VALUE from NUMERIC_STREAMS"
						+ " WHERE NUMERIC_STREAM_DEF_UUID = '"
						+ datadefUuid.toString()
						+ "' "
						+ " AND NUMERIC_STREAM_RECORD_POSITION>="
						+ startPosition
						+ " AND NUMERIC_STREAM_RECORD_POSITION<"
						+ endPosition
						+ " ) TO STDIN WITH BINARY";
				copy.copyOut(copyQry, dos);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This class writes the given data in binary to a given OutputStream
	 * 
	 * @author Arif Hossain, Kay Robbins
	 * 
	 */
	class WriteBinaryData implements Runnable {
		PipedOutputStream pout;
		long signalPosition;
		ByteBuffer template;
		int templateSize;
		double[] times;
		double[][] values;

		/**
		 * Creates a new WriteBinaryData object
		 * 
		 * @param pout
		 *            PipedOutputStream to write the data
		 * @param values
		 *            Values to be written
		 * @param signalPosition
		 *            Time of first signal. Value is automatically incremented
		 *            for each row
		 * @param template
		 *            Row template as a bytebuffer
		 */
		public WriteBinaryData(PipedOutputStream pout, double[][] values,
				double[] times, long signalPosition, ByteBuffer template) {
			this.pout = pout;
			this.values = values;
			this.times = times;
			this.signalPosition = signalPosition;
			this.template = template.duplicate();
			this.templateSize = template.capacity();
		}

		/**
		 * Execution thread to write the data in binary. A DataOutputStream is
		 * created on the given PipedOutputStream to write binary. Actual data
		 * values are inserted in the row template for faster insertion.
		 */
		public void run() {
			try {
				DataOutputStream dos = new DataOutputStream(pout);
				/********* HEADER # BEGIN::11+4+4 byte **********/
				dos.writeBytes("PGCOPY\n\377\r\n\0");
				dos.writeInt(0);
				dos.writeInt(0);
				/********* HEADER # END::11+4+4 byte **********/
				int rows = values.length;
				int cols = values[0].length;
				/*
				 * a buffer of 500 rows is used for writing data. the template
				 * is copied 500 times in a buffer then only the data samples
				 * are written in specific indices for each row. every row
				 * begins with 62 bytes by default. every data value is preceded
				 * by its size. that is why every data is placed after 12 bytes
				 * (4 bytes for size + 8 byte for data)
				 */
				for (int a = 0; a < cols; a = a + 500) {
					int width = Math.min(500, cols - a);
					ByteBuffer buffer = ByteBuffer.allocate(this.templateSize
							* width);
					for (int k = 0; k < width; k++) {
						template.putLong(26, signalPosition++);
						template.putDouble(38, times[k]);
						for (int b = 0; b < rows; b++) {
							template.putDouble(74 + b * 12, values[b][a + k]);
						}
						buffer.put(template.array());
					}
					dos.write(buffer.array());
					buffer.clear();
				}
				/********* TRAILER :: 4 byte ************/
				// dos.writeInt(-1);
				dos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private CopyManager copyMgr;
	private UUID datadefUuid;
	private Connection dbCon;
	private static final int INT_BYTES = 4;
	private static final int DOUBLE_BYTES = 8;
	private static final int LONG_BYTES = 8;
	private static final int SHORT_BYTES = 2;

	/**
	 * Creates a NumericData object
	 * 
	 * @param dbCon
	 *            Connection to the database
	 */
	public NumericStreams(Connection dbCon) throws Exception {
		try {
			this.dbCon = dbCon;
			copyMgr = ((org.postgresql.PGConnection) dbCon).getCopyAPI();
		} catch (Exception ex) {
			throw new MobbedException("Could not create numeric stream object");
		}
	}

	/**
	 * Creates a template row for NUMERIC_DATA table. All required binary flags,
	 * dimensions, and size of data in bytes is added to the template. Only the
	 * data field is kept empty for insertion
	 * 
	 * @param valueCount
	 *            Total number of values. Required to estimate the size of a row
	 *            in bytes
	 * @param datadefUuid
	 *            UUID of the datadef
	 * @return ByteBuffer A row template in a bytebuffer
	 * @throws Exception
	 */
	private ByteBuffer createTemplate(int valueCount, UUID datadefUuid)
			throws Exception {
		ByteBuffer template = null;
		try {
			int totalSize = SHORT_BYTES + INT_BYTES + 2 * LONG_BYTES
					+ INT_BYTES + LONG_BYTES + INT_BYTES + LONG_BYTES + 6
					* INT_BYTES + valueCount * (INT_BYTES + DOUBLE_BYTES);
			template = ByteBuffer.allocate(totalSize);
			/********* For every Row **********/
			template.putShort((short) 4); // # of fields
			template.putInt(2, LONG_BYTES * 2);
			template.putLong(6, datadefUuid.getMostSignificantBits());
			template.putLong(14, datadefUuid.getLeastSignificantBits());
			template.putInt(22, LONG_BYTES);
			// HERE:: RECORD_POSITION :: 8 bytes
			template.putInt(34, LONG_BYTES);
			// HERE:: RECORD_TIME :: 8 bytes
			int sizeOfData = valueCount * INT_BYTES + valueCount * DOUBLE_BYTES
					+ 5 * INT_BYTES;
			template.putInt(46, sizeOfData); // size of data in bytes per row
			template.putInt(50, 1); // dimension
			template.putInt(54, 0); // flag
			template.putInt(58, 701); // element_type
			template.putInt(62, valueCount); // columns (or size of
												// dimension[1])
			template.putInt(66, 1); // lower bound
			for (int a = 0; a < valueCount; a++) { // field size of array values
				template.putInt(70 + a * 12, 8);
			}
			/********* End of Rows **********/
		} catch (Exception ex) {
			throw ex;
		}
		return template;
	}

	/**
	 * get the UUID of DataDefs {@link edu.utsa.mobbed.Datadefs}
	 * 
	 * @return UUID - UUID of the DataDefs
	 */
	public UUID getDatadefUuid() {
		return datadefUuid;
	}

	/**
	 * Returns the last position of sampling for an element
	 * 
	 * @return long Last sampling time
	 * @throws Exception
	 */
	public long getMaxPosition() throws Exception {
		int maxPosition = 0;
		String selectQuery = "SELECT MAX(NUMERIC_STREAM_RECORD_POSITION) FROM NUMERIC_STREAMS WHERE"
				+ " NUMERIC_STREAM_DEF_UUID = ?";
		try {
			PreparedStatement selectStmt = dbCon.prepareStatement(selectQuery);
			selectStmt.setObject(1, datadefUuid, Types.OTHER);
			ResultSet rs = selectStmt.executeQuery();
			if (rs.next())
				maxPosition = rs.getInt("MAX");
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve max position");
		}
		return maxPosition;
	}

	/**
	 * Retrieves data samples between a given position range. Creates a separate
	 * thread to retrieve data from database as binary. The current execution
	 * thread reads only the required data and put them in a 2D-array
	 * 
	 * @param startPosition
	 *            Start time for retrieval
	 * @param endPosition
	 *            End time for retrieval
	 * @param elementCount
	 *            Total number of channels in this dataset
	 * @return double[][] A 2D-array of double values. Each row represents a
	 *         single time point and each values is a sample from each element.
	 */
	public double[][] retrieveByPosition(int startPosition, int endPosition,
			int elementCount) throws Exception {
		double[][] signal_data = new double[endPosition - startPosition][elementCount];
		try {
			// inputStream to read the data
			PipedInputStream pin = new PipedInputStream();
			// OutputStream to get data from database and write on the
			// inputStream
			PipedOutputStream pout = new PipedOutputStream(pin);
			// Start separate thread to read data from database
			ReadBinaryData rbd = new ReadBinaryData(pout, datadefUuid,
					startPosition, endPosition);
			Thread th = new Thread(rbd);
			th.start();
			/* Wrap PipedInputStream with DataInputStream to read data in binary */
			DataInputStream dis = new DataInputStream(pin);
			/********* HEADER # BEGIN::11+4+4 byte **********/
			dis.skipBytes(19);
			/********* HEADER # END::11+4+4 byte **********/
			/* read only the data bytes and skip everything else */
			short noOfFields = dis.readShort(); // No. of fields (should be =1)
			int index = 0;
			while (noOfFields == 1) { // if noOfFields=-1, end of data reached
				dis.skipBytes(INT_BYTES * 4);
				int dimension = dis.readInt(); // should be num of channels
				dis.skipBytes(INT_BYTES);
				for (int i = 0; i < dimension; i++) {
					dis.skipBytes(INT_BYTES);
					signal_data[index][i] = dis.readDouble();
				}
				index++;
				noOfFields = dis.readShort();
			}
			// close all streams
			dis.close();
			pout.close();
			pin.close();
		} catch (Exception ex) {
			throw new MobbedException("Could not retrieve numeric stream");
		}
		return signal_data;
	}

	/**
	 * Saves an array of data samples to the database. Creates an
	 * DataInputStream to the database, which writes data in binary. A
	 * WriteBinaryData object is created to run on a separate thread to write
	 * data in binary
	 * 
	 * @param values
	 *            2D-array of doubles values. Each row represent samples from an
	 *            element
	 * @param signalPosition
	 *            position of recording that sample
	 * @return boolean Success of storing and closing all threads
	 * @throws Exception
	 */
	public boolean save(double[][] values, double[] times, long signalPosition)
			throws Exception {
		boolean success = false;
		try {
			// The input stream for connecting to the table
			PipedInputStream pin = new PipedInputStream();
			// OutputStream for writing data on the inputStream
			PipedOutputStream pout = new PipedOutputStream(pin);
			// Start separate thread for writer
			WriteBinaryData wbd = new WriteBinaryData(pout, values, times,
					signalPosition, this.createTemplate(values.length,
							datadefUuid));
			Thread th = new Thread(wbd);
			th.start();
			// To read/write data in binary, wrap the PipedInputStream with a
			// DataInputStream and
			// connect to the table
			DataInputStream diStream = new DataInputStream(pin);
			copyMgr.copyIn(
					"COPY NUMERIC_STREAMS(NUMERIC_STREAM_DEF_UUID, "
							+ "NUMERIC_STREAM_RECORD_POSITION, NUMERIC_STREAM_RECORD_TIME, NUMERIC_STREAM_DATA_VALUE) FROM STDIN WITH BINARY",
					diStream);
			diStream.close();
			pin.close();
			success = true;
		} catch (Exception ex) {
			throw new MobbedException("Could not save numeric stream");
		}
		return success;
	}

	/**
	 * set the UUID of the DataDef
	 * 
	 * @param datadefUuid
	 *            UUID of the DataDefs
	 */
	public void reset(String datadefUuid) {
		this.datadefUuid = UUID.fromString(datadefUuid);
	}
}
