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

package galileo.samples;

import java.util.Map;

import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.feature.Feature;

/**
 * A simple demo showing the conversion of a Metadata instance to a custom
 * JSON format.  Future work will include an automatic serialization to JSON.
 *
 * @author malensek
 */
public class MetadataToJSON {

    private static int counter = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println(
                    "Usage: galileo.samples.MetadataToJSON <filename>");
            return;
        }

        String fileName = args[0];
        Map<String, Metadata> metas = ConvertNetCDF.readFile(fileName);
        for (Metadata meta : metas.values()) {
            MetadataToJSON.convert(meta);
        }
    }

    public static void convert(Metadata meta) {
        System.out.print("{");
        printValue("id", counter++ + "");
        printQuoted("name", meta.getName());
        printValue("hasSpatialProperties", meta.hasSpatialProperties());
        if (meta.hasSpatialProperties()) {
            SpatialProperties sp = meta.getSpatialProperties();
            if (sp.hasCoordinates()) {
                Coordinates c = sp.getCoordinates();
                String coords = "[" + c.getLongitude() + ", "
                    + c.getLatitude() + "]";
                printValue("spatialProperties", coords);
            } else if (sp.hasRange()) {
                //TODO range conversion?

            }
        }
        printValue("hasTemporalProperties", meta.hasTemporalProperties());

        for (Feature f : meta.getAttributes()) {
            printValue(f.getName(), f.getString());
        }

        //TODO features
        System.out.println("}");
    }

    private static void printQuoted(String a, String b) {
        final char quote = '"';
        System.out.print(quote + a + quote + " : " + quote + b + quote + ",");
    }

    private static void printValue(String a, Object b) {
        final char quote = '"';
        System.out.print(quote + a + quote + " : " + b + ",");
    }
}
