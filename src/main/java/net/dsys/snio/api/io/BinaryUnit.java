/**
 * Copyright 2014 Ricardo Padilha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dsys.snio.api.io;

/**
 * Similar to TimeUnit, to convert between binary units. Follows IEEE 1541-2002.
 * Zetta/zebi and yotta/yebi prefixes were removed because they cannot be
 * converted to/from bits reliably using 64-bit longs. Also, conversion between
 * ISO and IEEE prefixes (e.g., megabits to mebibits and vice-versa) always goes
 * through an intermediary step of bits.
 * 
 * @author Ricardo Padilha
 */
public enum BinaryUnit {

	/**
	 * Unit of conversion
	 */
	BITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toBits(n); }

		@Override public long toBits(final long n)      { return n; }
		@Override public long toKilobits(final long n)  { return n / (C1 / C0); }
		@Override public long toMegabits(final long n)  { return n / (C2 / C0); }
		@Override public long toGigabits(final long n)  { return n / (C3 / C0); }
		@Override public long toTerabits(final long n)  { return n / (C4 / C0); }
		@Override public long toPetabits(final long n)  { return n / (C5 / C0); }
		@Override public long toExabits(final long n)   { return n / (C6 / C0); }

		@Override public long toKibibits(final long n)  { return n / (B1 / B0); }
		@Override public long toMebibits(final long n)  { return n / (B2 / B0); }
		@Override public long toGibibits(final long n)  { return n / (B3 / B0); }
		@Override public long toTebibits(final long n)  { return n / (B4 / B0); }
		@Override public long toPebibits(final long n)  { return n / (B5 / B0); }
		@Override public long toExbibits(final long n)  { return n / (B6 / B0); }
	},

	/**
	 * 10^3 bits
	 */
	KILOBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toKilobits(n); }

		@Override public long toBits(final long n)      { return n * (C1 / C0); }
		@Override public long toKilobits(final long n)  { return n; }
		@Override public long toMegabits(final long n)  { return n / (C2 / C1); }
		@Override public long toGigabits(final long n)  { return n / (C3 / C1); }
		@Override public long toTerabits(final long n)  { return n / (C4 / C1); }
		@Override public long toPetabits(final long n)  { return n / (C5 / C1); }
		@Override public long toExabits(final long n)   { return n / (C6 / C1); }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 10^6 bits
	 */
	MEGABITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toMegabits(n); }

		@Override public long toBits(final long n)      { return n * (C2 / C0); }
		@Override public long toKilobits(final long n)  { return n * (C2 / C1); }
		@Override public long toMegabits(final long n)  { return n; }
		@Override public long toGigabits(final long n)  { return n / (C3 / C2); }
		@Override public long toTerabits(final long n)  { return n / (C4 / C2); }
		@Override public long toPetabits(final long n)  { return n / (C5 / C2); }
		@Override public long toExabits(final long n)   { return n / (C6 / C2); }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 10^9 bits
	 */
	GIGABITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toGigabits(n); }

		@Override public long toBits(final long n)      { return n * (C3 / C0); }
		@Override public long toKilobits(final long n)  { return n * (C3 / C1); }
		@Override public long toMegabits(final long n)  { return n * (C3 / C2); }
		@Override public long toGigabits(final long n)  { return n; }
		@Override public long toTerabits(final long n)  { return n / (C4 / C3); }
		@Override public long toPetabits(final long n)  { return n / (C5 / C3); }
		@Override public long toExabits(final long n)   { return n / (C6 / C3); }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 10^12 bits
	 */
	TERABITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toTerabits(n); }

		@Override public long toBits(final long n)      { return n * (C4 / C0); }
		@Override public long toKilobits(final long n)  { return n * (C4 / C1); }
		@Override public long toMegabits(final long n)  { return n * (C4 / C2); }
		@Override public long toGigabits(final long n)  { return n * (C4 / C3); }
		@Override public long toTerabits(final long n)  { return n; }
		@Override public long toPetabits(final long n)  { return n / (C5 / C4); }
		@Override public long toExabits(final long n)   { return n / (C6 / C4); }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 10^15 bits
	 */
	PETABITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toPetabits(n); }

		@Override public long toBits(final long n)      { return n * (C5 / C0); }
		@Override public long toKilobits(final long n)  { return n * (C5 / C1); }
		@Override public long toMegabits(final long n)  { return n * (C5 / C2); }
		@Override public long toGigabits(final long n)  { return n * (C5 / C3); }
		@Override public long toTerabits(final long n)  { return n * (C5 / C4); }
		@Override public long toPetabits(final long n)  { return n; }
		@Override public long toExabits(final long n)   { return n / (C6 / C5); }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 10^18 bits
	 */
	EXABITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toExabits(n); }

		@Override public long toBits(final long n)      { return n * (C6 / C0); }
		@Override public long toKilobits(final long n)  { return n * (C6 / C1); }
		@Override public long toMegabits(final long n)  { return n * (C6 / C2); }
		@Override public long toGigabits(final long n)  { return n * (C6 / C3); }
		@Override public long toTerabits(final long n)  { return n * (C6 / C4); }
		@Override public long toPetabits(final long n)  { return n * (C6 / C5); }
		@Override public long toExabits(final long n)   { return n; }

		@Override public long toKibibits(final long n)  { return BITS.toKibibits(toBits(n)); }
		@Override public long toMebibits(final long n)  { return BITS.toMebibits(toBits(n)); }
		@Override public long toGibibits(final long n)  { return BITS.toGibibits(toBits(n)); }
		@Override public long toTebibits(final long n)  { return BITS.toTebibits(toBits(n)); }
		@Override public long toPebibits(final long n)  { return BITS.toPebibits(toBits(n)); }
		@Override public long toExbibits(final long n)  { return BITS.toExbibits(toBits(n)); }
	},

	/**
	 * 2^10 bits
	 */
	KIBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toKibibits(n); }

		@Override public long toBits(final long n)      { return n * (B1 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n; }
		@Override public long toMebibits(final long n)  { return n / (B2 / B1); }
		@Override public long toGibibits(final long n)  { return n / (B3 / B1); }
		@Override public long toTebibits(final long n)  { return n / (B4 / B1); }
		@Override public long toPebibits(final long n)  { return n / (B5 / B1); }
		@Override public long toExbibits(final long n)  { return n / (B6 / B1); }
	},

	/**
	 * 2^20 bits
	 */
	MEBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toMebibits(n); }

		@Override public long toBits(final long n)      { return n * (B2 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n * (B2 / B1); }
		@Override public long toMebibits(final long n)  { return n; }
		@Override public long toGibibits(final long n)  { return n / (B3 / B2); }
		@Override public long toTebibits(final long n)  { return n / (B4 / B2); }
		@Override public long toPebibits(final long n)  { return n / (B5 / B2); }
		@Override public long toExbibits(final long n)  { return n / (B6 / B2); }
	},

	/**
	 * 2^30 bits
	 */
	GIBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toGibibits(n); }

		@Override public long toBits(final long n)      { return n * (B3 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n * (B3 / B1); }
		@Override public long toMebibits(final long n)  { return n * (B3 / B2); }
		@Override public long toGibibits(final long n)  { return n; }
		@Override public long toTebibits(final long n)  { return n / (B4 / B3); }
		@Override public long toPebibits(final long n)  { return n / (B5 / B3); }
		@Override public long toExbibits(final long n)  { return n / (B6 / B3); }
	},

	/**
	 * 2^40 bits
	 */
	TEBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toTebibits(n); }

		@Override public long toBits(final long n)      { return n * (B4 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n * (B4 / B1); }
		@Override public long toMebibits(final long n)  { return n * (B4 / B2); }
		@Override public long toGibibits(final long n)  { return n * (B4 / B3); }
		@Override public long toTebibits(final long n)  { return n; }
		@Override public long toPebibits(final long n)  { return n / (B5 / B4); }
		@Override public long toExbibits(final long n)  { return n / (B6 / B4); }
	},

	/**
	 * 2^50 bits
	 */
	PEBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toPebibits(n); }

		@Override public long toBits(final long n)      { return n * (B5 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n * (B5 / B1); }
		@Override public long toMebibits(final long n)  { return n * (B5 / B2); }
		@Override public long toGibibits(final long n)  { return n * (B5 / B3); }
		@Override public long toTebibits(final long n)  { return n * (B5 / B4); }
		@Override public long toPebibits(final long n)  { return n; }
		@Override public long toExbibits(final long n)  { return n / (B6 / B5); }
	},

	/**
	 * 2^60 bits
	 */
	EXBIBITS {
		@Override public long convert(final long n, final BinaryUnit u) { return u.toExbibits(n); }

		@Override public long toBits(final long n)      { return n * (B6 / B0); }
		@Override public long toKilobits(final long n)  { return BITS.toKilobits(toBits(n)); }
		@Override public long toMegabits(final long n)  { return BITS.toMegabits(toBits(n)); }
		@Override public long toGigabits(final long n)  { return BITS.toGigabits(toBits(n)); }
		@Override public long toTerabits(final long n)  { return BITS.toTerabits(toBits(n)); }
		@Override public long toPetabits(final long n)  { return BITS.toPetabits(toBits(n)); }
		@Override public long toExabits(final long n)   { return BITS.toExabits(toBits(n));  }

		@Override public long toKibibits(final long n)  { return n * (B6 / B1); }
		@Override public long toMebibits(final long n)  { return n * (B6 / B2); }
		@Override public long toGibibits(final long n)  { return n * (B6 / B3); }
		@Override public long toTebibits(final long n)  { return n * (B6 / B4); }
		@Override public long toPebibits(final long n)  { return n * (B6 / B5); }
		@Override public long toExbibits(final long n)  { return n; }
	};

	// bits to a byte
	private static final long A0 = 8L;

	// binary constants
	private static final long B0 = 1L;
	private static final long B1 = B0 * 1024L;
	private static final long B2 = B1 * 1024L;
	private static final long B3 = B2 * 1024L;
	private static final long B4 = B3 * 1024L;
	private static final long B5 = B4 * 1024L;
	private static final long B6 = B5 * 1024L;

	// SI constants
	private static final long C0 = 1L;
	private static final long C1 = C0 * 1000L;
	private static final long C2 = C1 * 1000L;
	private static final long C3 = C2 * 1000L;
	private static final long C4 = C3 * 1000L;
	private static final long C5 = C4 * 1000L;
	private static final long C6 = C5 * 1000L;

	public abstract long convert(long value, BinaryUnit sourceUnit);

	public abstract long toBits(long value);

	public abstract long toKilobits(long value);

	public abstract long toMegabits(long value);

	public abstract long toGigabits(long value);

	public abstract long toTerabits(long value);

	public abstract long toPetabits(long value);

	public abstract long toExabits(long value);

	public abstract long toKibibits(long value);

	public abstract long toMebibits(long value);

	public abstract long toGibibits(long value);

	public abstract long toTebibits(long value);

	public abstract long toPebibits(long value);

	public abstract long toExbibits(long value);

	public static long bytesToBits(final long value) {
		return value * A0;
	}

	public static long bitsToBytes(final long value) {
		return value / A0;
	}
}
