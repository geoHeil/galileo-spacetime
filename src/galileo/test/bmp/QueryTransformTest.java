package galileo.test.bmp;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import galileo.bmp.BitmapVisualization;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.dataset.Coordinates;

public class QueryTransformTest {
	
	public static void main(String[] args) throws IOException{
		List<Coordinates> polygon = new ArrayList<>();
		polygon.add(new Coordinates(40.597792003905454f, -104.77729797363281f));
		polygon.add(new Coordinates(40.43545015171254f, -104.77729797363281f));
		polygon.add(new Coordinates(40.43545015171254f, -105.10414123535156f));
		polygon.add(new Coordinates(40.597792003905454f, -105.10414123535156f));
		polygon.add(new Coordinates(40.597792003905454f, -104.77729797363281f));
		GeoavailabilityQuery query = new GeoavailabilityQuery(polygon);
		GeoavailabilityGrid grid = new GeoavailabilityGrid("9xjq8", 20);
		Random rand = new Random();
		int skipped = 0;
		for(int i = 0; i < 100000; i++)
			if(!grid.addPoint(rand.nextInt(1024), rand.nextInt(1024)))
				skipped++;
		System.out.println("skipped=" + skipped);
		BufferedImage b;
		b = BitmapVisualization.drawBitmap(
				QueryTransform.queryToGridBitmap(query, grid),
		        grid.getWidth(), grid.getHeight(), Color.RED);
		BitmapVisualization.imageToFile(b, "GridBitmap.png");
		b = BitmapVisualization.drawGeoavailabilityGrid(grid, Color.BLUE);
		BitmapVisualization.imageToFile(b, "GridQuery.png");
	}

}
