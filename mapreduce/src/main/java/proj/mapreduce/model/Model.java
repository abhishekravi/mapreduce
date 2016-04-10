package proj.mapreduce.model;

import proj.mapreduce.exception.BadDataException;

/**
 * Model class to hold files data.
 * @author Abhishek Ravi Chandran
 *
 */
public class Model {

	public float dryBulbTemp;
	public String record;

	/**
	 * method to set record data.
	 * @param data
	 * parsed data
	 * @param record
	 * whole record
	 * @throws BadDataException
	 */
	public void fill(String[] data, String record) throws BadDataException {
		try {
			this.dryBulbTemp = Float.parseFloat(data[8]);
			this.record = record;
		} catch (NumberFormatException e) {
			throw new BadDataException(e.getMessage());
		}
	}
	
	
}
