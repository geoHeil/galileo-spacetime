/*
Copyright (c) 2014, Colorado State University
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

package galileo.bmp;

import java.util.Iterator;

import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * A thin wrapper around {@link com.googlecode.javaewah.EWAHCompressedBitmap}
 * to enable us to change bitmap implementations if need be.
 *
 * @author malensek
 */
public class Bitmap implements Iterable<Integer> {

    private EWAHCompressedBitmap bmp;

    public Bitmap() {
        bmp = new EWAHCompressedBitmap();
    }

    private Bitmap(EWAHCompressedBitmap bmp) {
        this.bmp = bmp;
    }

    /**
     * Sets the specified bit(s) in the index.
     *
     * @param bit bit to set (to 1, 'on', etc.)
     *
     * @return true if the bit could be set, false otherwise.  In some cases,
     * the underlying bitmap implementation may disallow updates, causing this
     * method to return false.
     */
    public boolean set(int bit) {
        return bmp.set(bit);
    }

    public Bitmap or(Bitmap otherBitmap) {
        return new Bitmap(this.bmp.or(otherBitmap.bmp));
    }

    public Bitmap xor(Bitmap otherBitmap) {
        return new Bitmap(this.bmp.xor(otherBitmap.bmp));
    }

    public Bitmap and(Bitmap otherBitmap) {
        return new Bitmap(this.bmp.and(otherBitmap.bmp));
    }

    public boolean intersects(Bitmap otherBitmap) {
        return this.bmp.intersects(otherBitmap.bmp);
    }

    public int[] toArray() {
        return this.bmp.toArray();
    }

    /**
     * Given an array of raw bytes, convert the bytes into a compressed bitmap
     * representation with 2D characteristics.  This method provides
     * functionality slightly different from a direct conversion; it assumes
     * that the bitmaps in question are two-dimensional (represented by a 1D bit
     * stream) and have geometric properties (height, width, x, y, etc.).
     * <p>
     * One assumption this method makes is that incoming bytes are word-aligned;
     * that is, the width and height of the bytes being converted must be
     * evenly-divisible by 64.
     *
     * @param bytes The raw bytes to convert
     * @param x The x-coordinate to begin placing the bytes
     * @param y The y-coordinate to begin placing the bytes
     * @param width The width of the raw data in the array
     * @param height The height of the raw data in the array
     * @param canvasWidth The desired width of the bitmap 'canvas' the bytes
     * will be placed on
     * @param canvasHeight The desired height of the bitmap 'canvas' the bytes
     * will be placed on
     *
     * @return Bitmap representation of the bytes provided, integrated onto the
     * bitmap 'canvas' with the width, height, and positions provided.
     */
    /*public static Bitmap fromBytes(byte[] bytes, int x, int y,
            int width, int height,
            int canvasWidth, int canvasHeight) {

        EWAHCompressedBitmap bmp = new EWAHCompressedBitmap();

         Shift through the bitmap to the first place we need to draw. 
        int idx = canvasWidth * y + x;
        int shift = idx % 64;
        int skipWords = (idx - shift) / 64;
        bmp.addStreamOfEmptyWords(false, skipWords);

         Convert raw image data to binary words 
        long[] words = bytesToWords(bytes);

        int lines = height;
        int wordsPerLine = width / 64;
        for (int line = 0; line < lines; ++line) {
            int wordIdx = line * wordsPerLine;
            bmp.addStreamOfLiteralWords(words, wordIdx, wordsPerLine);
            bmp.addStreamOfEmptyWords(false, (canvasWidth / 64) - wordsPerLine);
        }

        return new Bitmap(bmp);
    }*/
    
    
    public static Bitmap fromBytes(byte[] bytes, int x, int y,
            int width, int height,
            int canvasWidth, int canvasHeight) {

        EWAHCompressedBitmap bmp = new EWAHCompressedBitmap();
        //if the bounded rectangle of the polygon does not intersect the grid canvas
        if(x >= canvasWidth || y >= canvasHeight || x + width < 0 || y + height < 0) {
        	for(int line = 0; line < canvasHeight; line++)
        		bmp.addStreamOfEmptyWords(false, canvasWidth/64);
        	return new Bitmap(bmp);
        }
        
        int intersectedX = (x < 0) ? 0 : x;
        int intersectedY = (y < 0) ? 0 : y;
        //intersection width of the bounding rectangle and the grid
        int intersectedWidth = (x < 0) ? ((width + x > canvasWidth) ? canvasWidth : width + x) 
        							: ((x + width > canvasWidth)? canvasWidth - x : width);
        if(intersectedWidth > canvasWidth)
        	intersectedWidth = canvasWidth;

        /* Shift through the bitmap to the first place we need to draw. */
        bmp.addStreamOfEmptyWords(false, canvasWidth * intersectedY / 64);
        
        int shift = intersectedX % 64;
        if(intersectedWidth % 64 != 0)
        	intersectedWidth += shift;
        int skipWords = (intersectedX - shift) / 64; //definitely positive or zero
        /* Convert raw image data to binary words */
        long[] words = bytesToWords(bytes);

        int wordsPerLine = width / 64;
        for (int line = intersectedY; line < canvasHeight; line++) {
        	int transformedLine = line - y;
        	int transformedX = intersectedX - x;
            int wordIdx = transformedLine * wordsPerLine + transformedX/64;
            bmp.addStreamOfEmptyWords(false, skipWords);
            if(transformedLine >= 0 && transformedLine < height)
            	bmp.addStreamOfLiteralWords(words, wordIdx, intersectedWidth/64);
            else
            	bmp.addStreamOfEmptyWords(false, intersectedWidth/64);
            bmp.addStreamOfEmptyWords(false, (canvasWidth - intersectedWidth - skipWords*64)/64);
        }
        return new Bitmap(bmp);
    }

    /**
     * Do a direct conversion of a byte array to 64-bit words (longs).
     *
     * @return an array of 64-bit words.
     */
    private static long[] bytesToWords(byte[] bytes) {
        long[] words = new long[bytes.length / 64];
        for (int i = 0; i < bytes.length / 64; ++i) {
            for (int j = 0; j < 64; ++j) {
                if (bytes[i * 64 + j] < 0) {
                    words[i] |= (1l << j);
                }
            }
        }
        return words;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bmp == null) ? 0 : bmp.hashCode());
		return result;
	}

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Bitmap b = (Bitmap) obj;
        return this.bmp.equals(b.bmp);
    }

    @Override
    public Iterator<Integer> iterator() {
        return bmp.iterator();
    }
}
