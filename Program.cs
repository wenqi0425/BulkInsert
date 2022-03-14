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

            #region title.basics.tsv

            //Console.WriteLine("try to read all lines of title.basics.tsv");
            //var allResults = ReadAllTitles(@"C:\title.basics.tsv");
            //Console.WriteLine("Has read all lines of title.basics.tsv");

            //Console.WriteLine("Insert TitleBasics.");
            //InsertTitleBasic((List<TitleBasic>)allResults["titles"], connection);
            //Console.WriteLine("TitleBasics Table is done.");

            //Console.WriteLine("Insert Genres Table.");
            //var g = (HashSet<string>)allResults["genres"];
            //GenresInsert genresInsert = new GenresInsert();
            //genresInsert.InsertGenre( connection, g);
            //Console.WriteLine("Genres Table is done.");

            //Console.WriteLine("Insert TitlesAndGenres Table.");
            //var titleAndGenres = (Dictionary<string, List<int>>)allResults["TitlesAndGenres"];
            //InsertTitlesAndGenres(titleAndGenres, connection);
            //Console.WriteLine("TitleAndGenres Table is done.");

            #endregion


            #region name.basics.tsv

            Console.WriteLine("try to read all lines of name.basics.tsv");
            var allNameBasics = ReadAllNames(@"C:\name.basics.tsv");
            Console.WriteLine("Has read all lines of name.basics.tsv");

            Console.WriteLine("Insert NameBasics.");
            InsertNameBasic((List<NameBasic>)allNameBasics["names"], connection);
            Console.WriteLine("NameBasics Table is done.");

            Console.WriteLine("Insert Professions.");
            var professions = (HashSet<string>)allNameBasics["professions"];
            ProfessionsInsert professionsInsert = new ProfessionsInsert();
            professionsInsert.InsertProfession(connection, professions);
            Console.WriteLine("Professions Table is done.");

            Console.WriteLine("Insert NameAndProfessions.");
            //NameAndProfessionsInsert nameAndProfessionsInsert = new NameAndProfessionsInsert();
            var nameAndProfessions = (Dictionary<String, List<int>>)allNameBasics["NameAndProfessions"];
            //nameAndProfessionsInsert.InsertNameAndProfessions(connection, nameAndProfessions.ToList());
            InsertNameAndProfessions(nameAndProfessions, connection);
            Console.WriteLine("NameAndProfessions Table is done.");

            Console.WriteLine("Insert NameAndTitles");
            var nameAndTitles = (Dictionary<String, String[]>)allNameBasics["NameAndTitles"];
            //NameAndTitlesInsert nameAndTitlesInsert = new NameAndTitlesInsert();
            //nameAndTitlesInsert.InsertNameAndTitles(connection, nameAndTitles);
            InsertNameAndTitles(nameAndTitles,connection);
            Console.WriteLine("NameAndTitles Table is done");
            #endregion

            connection.Close();

            //List<NameBasic> allNames = ReadAllNames(@"C:\name.basics.tsv", 1000000);
            //Console.WriteLine("Has read all lines of name.basics.tsv");
        }

        #region title.basics.tsv methods

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

        #endregion

        public static Dictionary<String, Object> ReadAllNames(string filePath)
        {
            Dictionary<String, Object> results = new Dictionary<string, object>();
            List<NameBasic> allNames = new List<NameBasic>();
            
            HashSet<string> allProfessions = new HashSet<string>();
            Dictionary<String, String[]> nameProfessionsDictionary = new Dictionary<string, String[]>();
            Dictionary<String, List<int>> nameProfessionPKDictionary = new Dictionary<string, List<int>>();

            Dictionary<String, String[]> nameTitlesDictionary = new Dictionary<string, String[]>();

            int counter = 0;
            foreach (string line in File.ReadLines(filePath))
            {
                if (counter != 0)
                {
                    string[] splitLine = line.Split("\t");
                    string profession = splitLine[splitLine.Length - 2];
                    string[] professions = profession.Split(",");
                    string knownForTitle = splitLine[splitLine.Length - 1];
                    string[] knownForTitles = knownForTitle.Split(",");
                    
                    foreach (var item in professions)
                    {
                        allProfessions.Add(item);
                    }

                    if (splitLine.Length == 6)
                    {
                        allNames.Add(new NameBasic(splitLine));
                    }
                    else
                    {
                        Console.WriteLine(line);
                    }

                    nameProfessionsDictionary.Add(splitLine[0], professions);
                    nameTitlesDictionary.Add(splitLine[0], knownForTitles);
                }
                counter++;

                if (line == null)
                {
                    break;
                }
            }

            List<String> professionsList = new List<String>(allProfessions);

            foreach (KeyValuePair<String, String[]> entry in nameProfessionsDictionary)
            {
                List<int> professionIds = new List<int>();
                String[] nameProfessions = entry.Value;
                foreach (var profession in nameProfessions)
                {
                    int index = professionsList.IndexOf(profession);
                    if (index == -1)
                    {
                        Console.WriteLine("Profession isn't in the profession list.");
                    }
                    professionIds.Add(index);
                }

                nameProfessionPKDictionary.Add(entry.Key, professionIds);
            }

            results.Add("names", allNames);
            results.Add("professions", allProfessions);
            results.Add("NameAndProfessions", nameProfessionPKDictionary);
            results.Add("NameAndTitles", nameTitlesDictionary);
            return results;
        }

        public static void InsertNameBasic(List<NameBasic> dataList, SqlConnection connection)
        {
            int length = dataList.Count;
            Console.WriteLine(length);
            int range = 100000;
            var iteration = length / range;
            var remaining = length % range;
            var myInserter = new NameBasicsInsert();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                myInserter.InsertName(connection, dataList.GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }

            myInserter.InsertName(connection, dataList.GetRange(iteration * range, remaining));

            //SqlSelector mySelector = new SqlSelector();
            //foreach (TitleBasic title in mySelector.SelectAllTitles(connection))
            //{
            //    //Console.WriteLine(title.originalTitle);
            //}            
        }

        public static void InsertNameAndTitles(Dictionary<String, String[]> nameAndProfessionsPair, SqlConnection connection)
        {
            int length = nameAndProfessionsPair.Count;
            Console.WriteLine(length);
            int range = 100000;
            var iteration = length / range;
            var remaining = length % range;
            var myInserter = new NameAndTitlesInsert();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                myInserter.InsertNameAndTitles(connection, nameAndProfessionsPair.ToList().GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }

            myInserter.InsertNameAndTitles(connection, nameAndProfessionsPair.ToList().GetRange(iteration * range, remaining));
        }

        public static void InsertNameAndProfessions(Dictionary<String, List<int>> nameAndProfessionsDictionary, SqlConnection connection)
        {
            int length = nameAndProfessionsDictionary.Count;
            Console.WriteLine(length);
            int range = 100000;
            var iteration = length / range;
            var remaining = length % range;
            var myInserter = new NameAndProfessionsInsert();

            for (int i = 0; i < iteration; i++)
            {
                DateTime beforeTime = DateTime.Now;
                myInserter.InsertNameAndProfessions(connection, nameAndProfessionsDictionary.ToList().GetRange(i * range, range));
                DateTime afterTime = DateTime.Now;
                Console.WriteLine("It took: " + afterTime.Subtract(beforeTime).TotalSeconds + " seconds");
            }

            myInserter.InsertNameAndProfessions(connection, nameAndProfessionsDictionary.ToList().GetRange(iteration * range, remaining));
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

        //public class TitleGener
        //{
        //    private String titleID;
        //    private int generID;
        //    public TitleGener(String titleID, int generID)
        //    {
        //        this.titleID = titleID;
        //        this.generID = generID;
        //    }
        //}
    }
}
