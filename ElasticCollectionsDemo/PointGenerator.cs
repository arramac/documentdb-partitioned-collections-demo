namespace ElasticCollectionsDemo
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft;
    using Newtonsoft.Json;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Spatial;

    public static class PointGenerator
    {
        private static List<Point> cities = new List<Point>();

        public static void Intialize()
        {
            System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            int num = 0;
            foreach (string line in File.ReadLines(@"citylocations.csv").Skip(1))
            {
                string[] parts = line.Split(',');
               
                //Reads Country,City,AccentCity,Region,Population,Latitude,Longitude
                cities.Add(new Point(double.Parse(parts[6]), double.Parse(parts[5])));
            }

            Console.WriteLine("loaded in {0}. size = {1}", watch.Elapsed, cities.Count);
        }

        public static Point GetRandomPoint(int seed)
        {
            Random random = new Random(seed);
            Point location = cities[random.Next(cities.Count)];

            double longFuzz = random.Next(-1000, 1000) * 0.00001;
            double latFuzz = random.Next(-1000, 1000) * 0.00001;

            return new Point(location.Position.Longitude + longFuzz, location.Position.Latitude + latFuzz);
        }
    }
}
