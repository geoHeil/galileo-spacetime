package galileo.dht.hash;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import galileo.dataset.Metadata;
import galileo.dataset.TemporalProperties;

public class TemporalHash implements HashFunction<Metadata> {
	private Random random = new Random();
	private int temporalType;

	/**
	 * @param temporalType:
	 * Must be one of the constants of java.util.Calendar
	 * @throws HashException
	 * If temporalType does not match the one of the supported constants.
	 */
	public TemporalHash(int temporalType) throws HashException {
		this.temporalType = temporalType;
		List<Integer> temporalTypes = Arrays.asList(new Integer[] { Calendar.DAY_OF_MONTH, Calendar.DAY_OF_WEEK,
				Calendar.DAY_OF_YEAR, Calendar.HOUR, Calendar.HOUR_OF_DAY, Calendar.WEEK_OF_MONTH,
				Calendar.WEEK_OF_YEAR, Calendar.MONTH, Calendar.YEAR });
		if (!temporalTypes.contains(temporalType)) {
			throw new HashException("Unsupported temporal type for hashing.");
		}
	}

	@Override
	public BigInteger hash(Metadata data) throws HashException {
		TemporalProperties temporalProps = data.getTemporalProperties();
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(temporalProps.getStart());
		switch (this.temporalType) {
		case Calendar.DAY_OF_MONTH:
			return BigInteger.valueOf(c.get(Calendar.DAY_OF_MONTH));
		case Calendar.DAY_OF_WEEK:
			return BigInteger.valueOf(c.get(Calendar.DAY_OF_WEEK));
		case Calendar.DAY_OF_YEAR:
			return BigInteger.valueOf(c.get(Calendar.DAY_OF_YEAR));
		case Calendar.HOUR:
			return BigInteger.valueOf(c.get(Calendar.HOUR));
		case Calendar.HOUR_OF_DAY:
			return BigInteger.valueOf(c.get(Calendar.HOUR_OF_DAY));
		case Calendar.WEEK_OF_MONTH:
			return BigInteger.valueOf(c.get(Calendar.WEEK_OF_MONTH));
		case Calendar.WEEK_OF_YEAR:
			return BigInteger.valueOf(c.get(Calendar.WEEK_OF_YEAR));
		case Calendar.MONTH:
			return BigInteger.valueOf(c.get(Calendar.MONTH));
		case Calendar.YEAR:
			return BigInteger.valueOf(c.get(Calendar.YEAR) % 100);
		default:
			throw new HashException("Unsupported temporal type for hashing.");
		}
	}

	@Override
	public BigInteger maxValue() {
		switch (this.temporalType) {
		case Calendar.DAY_OF_MONTH:
			return BigInteger.valueOf(31);
		case Calendar.DAY_OF_WEEK:
			return BigInteger.valueOf(7);
		case Calendar.DAY_OF_YEAR:
			return BigInteger.valueOf(366);
		case Calendar.HOUR:
			return BigInteger.valueOf(12);
		case Calendar.HOUR_OF_DAY:
			return BigInteger.valueOf(24);
		case Calendar.WEEK_OF_MONTH:
			return BigInteger.valueOf(5);
		case Calendar.WEEK_OF_YEAR:
			return BigInteger.valueOf(53);
		case Calendar.MONTH:
			return BigInteger.valueOf(12);
		case Calendar.YEAR:
			return BigInteger.valueOf(100);
		default:
			return BigInteger.valueOf(31);
		}
	}

	@Override
	public BigInteger randomHash() {
		switch (this.temporalType) {
		case Calendar.DAY_OF_MONTH:
			return BigInteger.valueOf(random.nextInt(31));
		case Calendar.DAY_OF_WEEK:
			return BigInteger.valueOf(random.nextInt(7));
		case Calendar.DAY_OF_YEAR:
			return BigInteger.valueOf(random.nextInt(366));
		case Calendar.HOUR:
			return BigInteger.valueOf(random.nextInt(12));
		case Calendar.HOUR_OF_DAY:
			return BigInteger.valueOf(random.nextInt(24));
		case Calendar.WEEK_OF_MONTH:
			return BigInteger.valueOf(random.nextInt(5));
		case Calendar.WEEK_OF_YEAR:
			return BigInteger.valueOf(random.nextInt(53));
		case Calendar.MONTH:
			return BigInteger.valueOf(random.nextInt(12));
		case Calendar.YEAR:
			return BigInteger.valueOf(random.nextInt(100));
		default:
			return BigInteger.valueOf(random.nextInt(31));
		}
	}
}
