using BulkInsert.Models;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;


namespace BulkInsert
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("building sql connection");
            SqlConnection connection = new SqlConnection(@"server=(localdb)\MSSQLLocalDB;database=RawDB;user id=Raw;password=123456");
            connection.Open();
            Console.WriteLine("connection is opened");

            Console.WriteLine("try to read all lines of title.basics.tsv");
            var allResults = ReadAllTitles(@"C:\title.basics.tsv");
            Console.WriteLine("Has read all lines of title.basics.tsv");

            //Console.WriteLine("Insert TitleBasics.");
            //InsertTitleBasic((List<TitleBasic>)allResults["titles"], connection);
            //Console.WriteLine("TitleBasics Table is done.");

            Console.WriteLine("Insert Genres Table.");
            var g = (HashSet<string>)allResults["genres"];
            GenresInsert genresInsert = new GenresInsert();
            genresInsert.InsertGenre( connection, g);
            Console.WriteLine("Genres Table is done.");

            //Console.WriteLine("Insert Conjunction Table.");
            //var titleAndGenres = (Dictionary<string, List<int>>)allResults["TitlesAndGenres"];
            //InsertTitlesAndGenres(titleAndGenres, connection);

            Console.WriteLine("TitleAndGenres Table is done.");
            connection.Close();

            //List<NameBasic> allNames = ReadAllNames(@"C:\name.basics.tsv", 1000000);
            //Console.WriteLine("Has read all lines of name.basics.tsv");

            //List<TitleBasic> allTitles = ReadAllTitles(@"C:\title.akas.tsv", 1000000);
            //Console.WriteLine("Has read all lines of title.akas.tsv");

            //List<TitleBasic> allTitles = ReadAllTitles(@"C:\title.crew.tsv", 1000000);
            //Console.WriteLine("Has read all lines of title.crew.tsv");

            //List<TitleBasic> allTitles = ReadAllTitles(@"C:\title.principals.tsv", 1000000);
            //Console.WriteLine("Has read all lines of title.principals.tsv");
        
        }

        public static Dictionary<String, Object> ReadAllTitles(string filePath)
        {
            Dictionary<String, Object> results = new Dictionary<string, object>();
            List<TitleBasic> allTitles = new List<TitleBasic>();
            HashSet<string> allGeners = new HashSet<string>();
            Dictionary<String, String[]> titleGenersDictionary = new Dictionary<string, String[]>();

            Dictionary<String, List<int>> titleGenerPKDictionary = new Dictionary<string, List<int>>();

            int counter = 0;
            foreach (string line in File.ReadLines(filePath))
            {
                if (counter != 0)
                {
                    string[] splitLine = line.Split("\t");
                    string gener = splitLine[splitLine.Length - 1];
                    string[] geners = gener.Split(",");
                    foreach (var item in geners)
                    {
                        allGeners.Add(item);
                    }

                    if (splitLine.Length == 9)
                    {
                        allTitles.Add(new TitleBasic(splitLine));
                    }
                    else
                    {
                        Console.WriteLine(line);
                    }

                    // keep title and geners info. in a dictionary
                    titleGenersDictionary.Add(splitLine[0], geners);

                }
                counter++;

                if (line == null)
                {
                    break;
                }
            }

            // mapping a gener as a FK in conjunctionTable pointing a Gener. 
            List<String> genersList = new List<String>(allGeners);

            foreach (KeyValuePair<String, String[]> entry in titleGenersDictionary)
            {
                List<int> generIds = new List<int>();
                String[] titleGeners = entry.Value;
                foreach (var gener in titleGeners)
                {
                    int index = genersList.IndexOf(gener);
                    generIds.Add(index);
                }

                titleGenerPKDictionary.Add(entry.Key, generIds);
            }

            results.Add("titles", allTitles);
            results.Add("genres", allGeners);
            results.Add("TitlesAndGenres", titleGenerPKDictionary);
            return results;
        }


        public static void InsertTitleBasic(List<TitleBasic> dataList, SqlConnection connection)
        {
            int length = dataList.Count;
            Console.WriteLine(length);
            int range = 100000;
            var iteration = length / range;
            var remaining = length % range;
            IInsert myInserter = new TitleBasicsInsert();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                myInserter.InsertTitleBasic(connection, dataList.GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }

            myInserter.InsertTitleBasic(connection, dataList.GetRange(iteration * range, remaining));

            //SqlSelector mySelector = new SqlSelector();
            //foreach (TitleBasic title in mySelector.SelectAllTitles(connection))
            //{
            //    //Console.WriteLine(title.originalTitle);
            //}            
        }

        public static void InsertTitlesAndGenres(Dictionary<string, List<int>> titleAndGenres, SqlConnection connection)
        {
            int length = titleAndGenres.Count;
            Console.WriteLine(length);
            int range = 100000;
            var iteration = length / range;
            var remaining = length % range;

            TitleAndGenresInsert titleAndGenresInsert = new TitleAndGenresInsert();
            List<KeyValuePair<string, List<int>>> list = titleAndGenres.ToList();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                titleAndGenresInsert.InsertTitleAndGenre(connection, list.GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }
        }



        /*
        public void InsertDB<T>(List<T> dataList, SqlConnection connection)
        {
            int length = dataList.Count;
            Console.WriteLine(length);
            int range = 1000000;
            var iteration = length / range;
            var remaining = length % range;
            IInsert myInserter = new BulkTitleBasic();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                myInserter.InsertTitleBasic(connection, dataList.GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }

            myInserter.InsertTitleBasic(connection, dataList.GetRange(iteration * range, remaining));


            //SqlSelector mySelector = new SqlSelector();
            //foreach (TitleBasic title in mySelector.SelectAllTitles(connection))
            //{
            //    //Console.WriteLine(title.originalTitle);
            //}

            connection.Close(); 
        }
        */

        public class TitleGener
        {
            private String titleID;
            private int generID;
            public TitleGener(String titleID, int generID)
            {
                this.titleID = titleID;
                this.generID = generID;
            }
        }

        public static List<NameBasic> ReadAllNames(string filePath, int maxRows)
        {
            List<NameBasic> allNames = new List<NameBasic>();

            int counter = 0;
            foreach (string line in File.ReadLines(filePath))
            {
                if (counter != 0)
                {
                    string[] splitLine = line.Split("\t");
                    if (splitLine.Length == 9)
                    {
                        allNames.Add(new NameBasic(splitLine));
                    }
                }
                counter++;

                if (counter >= maxRows)
                {
                    break;
                }
            }

            return allNames;
        }
    }
}
