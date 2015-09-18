/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.dht.hash;

import galileo.util.Checksum;

import java.math.BigInteger;
import java.util.Random;

/**
 * Provides a very small (configurable) hash space for testing purposes.
 *
 * @author malensek
 */
public class TinyHash implements HashFunction<byte[]> {

    private int size = 0;
    private Checksum checksum = new Checksum();
    private Random random = new Random();

    public TinyHash(int size) {
        this.size = size;
    }

    @Override
    public BigInteger hash(byte[] data) {
        BigInteger result = new BigInteger(1, checksum.hash(data));
        result = result.mod(BigInteger.valueOf(size));
        return result;
    }

    @Override
    public BigInteger maxValue() {
        return BigInteger.valueOf(size);
    }

    @Override
    public BigInteger randomHash() {
        int randInt = random.nextInt(size);
        return BigInteger.valueOf(randInt);
    }
}
