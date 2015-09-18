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

import java.io.File;
import java.io.IOException;

import java.util.List;

import ucar.ma2.*;
import ucar.nc2.*;

public class ReadNetCDF {
    public static void main(String[] args)
    throws Exception {
        File dir = new File(args[0]);
        for (File f : dir.listFiles()) {
            // Make sure we only try to read grib files
            String fileName = f.getName();
            String ext = fileName.substring(fileName.lastIndexOf('.') + 1,
                    fileName.length());
            if (ext.equals("grb") || ext.equals("bz2")) {
                ReadNetCDF.readFile(f.getAbsolutePath());
            }
        }
    }

    public static void readFile(String file)
    throws Exception {
        NetcdfFile n = NetcdfFile.open(file);

        Array humi = getVals(n, "Maximum_Relative_Humumidity");
        Array temp = getVals(n, "Temperature_surface");
        Array wind = getVals(n, "Surface_wind_gust");
        Array snow = getVals(n, "Snow_depth");

        if (humi == null || temp == null || wind == null || snow == null) {
            return;
        }

        while (humi.hasNext() && temp.hasNext()
                && wind.hasNext() && snow.hasNext()) {

            double h = humi.nextDouble();
            double t = temp.nextDouble();
            double w = wind.nextDouble();
            double s = snow.nextDouble();

            System.out.println("h=" + h +", t=" + t + ", w=" + w + ", s=" + s);
        }
    }

    public static Array getVals(NetcdfFile file, String varName)
    throws IOException, InvalidRangeException {
        Variable v = file.findVariable(varName);
        if (v == null) {
            return null;
        }
        List<Dimension> dims = v.getDimensions();
        int numDims = dims.size();

        int[] origin = new int[numDims];
        for (int i = 0; i < numDims; ++i) {
            origin[i] = 0;
        }

        int[] size = new int[numDims];
        for (int i = 0; i < numDims; ++i) {
            size[i] = dims.get(i).getLength();
        }

        return v.read(origin, size);
    }
}
