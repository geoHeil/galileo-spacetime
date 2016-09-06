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

import galileo.dataset.Metadata;
import galileo.util.FileNames;
import galileo.util.Pair;

import java.io.File;
import java.util.Map;

import ucar.nc2.util.DiskCache;

public class DumpNetCDF {
    public static void main(String[] args)
    throws Exception {
        DiskCache.setCachePolicy(true);

        File f = new File(args[0]);
        Pair<String, String> nameParts = FileNames.splitExtension(f);
        String ext = nameParts.b;
        if (ext.equals("grb") || ext.equals("bz2") || ext.equals("gz")) {
            Map<String, Metadata> metaMap
                = ConvertNetCDF.readFile(f.getAbsolutePath());

            /* Don't cache more than 1 GB: */
            DiskCache.cleanCache(1073741824, null);

            /*String[] attribs = { "temperature_surface",
                "total_cloud_cover_entire_atmosphere",
                "visibility_surface",
                "pressure_surface",
                "categorical_snow_yes1_no0_surface",
                "categorical_rain_yes1_no0_surface",
                "relative_humidity_zerodegc_isotherm" };*/
            String[] attribs = {"U-component_of_wind"};
            Metadata m = metaMap.get("9x");
            System.out.print("9x@"
                    + m.getTemporalProperties().getStart() + "\t");
            for (String attrib : attribs) {
                System.out.print(m.getAttribute(attrib).getString() + "\t");
            }
            System.out.println();
        }
    }
}
